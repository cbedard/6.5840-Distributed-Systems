package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	ClientId  int
	RequestId int
	Type      string
	Key       string
	Value     string
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// lab 4b, raftkv attributes
	maxraftstate int // snapshot if log grows this big
	db           map[string]string
	waitingOps   map[int]chan Op // operations waiting to be committed by raft layer
	processed    map[int]int     // clientId -> last processed requestId

	// lab 5 attributes
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd
	mck      *shardctrler.Clerk
	config   shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// wrong group / cfg staleness check
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if args.RequestId <= kv.processed[args.ClientId] {
		reply.Value = kv.db[args.Key] // already committed
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	commandIndex, _, isLeader := kv.rf.Start(Op{args.ClientId, args.RequestId, "Get", args.Key, ""})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ch := make(chan Op)
	kv.waitingOps[commandIndex] = ch
	kv.mu.Unlock()

	committedOp := <-ch
	if committedOp.ClientId == args.ClientId && committedOp.RequestId == args.RequestId {
		reply.Value = committedOp.Value
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.putAppend(args, reply, "Put")
}

func (kv *ShardKV) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.putAppend(args, reply, "Append")
}

func (kv *ShardKV) putAppend(args *PutAppendArgs, reply *PutAppendReply, opType string) {
	kv.mu.Lock()
	// wrong group / cfg staleness check
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if args.RequestId <= kv.processed[args.ClientId] {
		reply.Err = OK // already committed
		kv.mu.Unlock()
		return
	}

	commandIndex, _, isLeader := kv.rf.Start(Op{args.ClientId, args.RequestId, opType, args.Key, args.Value})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ch := make(chan Op)
	kv.waitingOps[commandIndex] = ch
	kv.mu.Unlock()

	committedOp := <-ch
	if committedOp.ClientId == args.ClientId && committedOp.RequestId == args.RequestId {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) ApplyOperations() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.SnapshotValid {
			// restore state from snapshot
			kv.decodeSnapshot(msg.Snapshot)

		} else if msg.CommandValid {
			op := msg.Command.(Op)

			if op.RequestId > kv.processed[op.ClientId] {
				if op.Type == "Put" {
					kv.db[op.Key] = op.Value
				}
				if op.Type == "Append" {
					kv.db[op.Key] += op.Value
				}

				kv.processed[op.ClientId] = op.RequestId
			}

			if op.Type == "Get" {
				op.Value, _ = kv.db[op.Key]
			}

			// because a committed operation may have dropped on the way back to client,
			// can dupe the response as long as we dont dupe the write op
			if ch, ok := kv.waitingOps[msg.CommandIndex]; ok {
				ch <- op
				delete(kv.waitingOps, msg.CommandIndex)
			}

			// hit max rf state, take snapshot
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				snapshot := kv.encodeSnapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) pollConfig() {
	for {
		kv.mu.Lock()
		kv.config = kv.mck.Query(-1)

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// must be called with lock
func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.processed)

	return w.Bytes()
}

// must be called with lock
func (kv *ShardKV) decodeSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var processed map[int]int
	if d.Decode(&db) != nil || d.Decode(&processed) != nil {
		panic("ERROR Decoding")
	}

	kv.db = db
	kv.processed = processed
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.db = make(map[string]string)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.waitingOps = make(map[int]chan Op)
	kv.processed = make(map[int]int)

	go kv.ApplyOperations() // waits for raft commits async
	go kv.pollConfig()

	return kv
}
