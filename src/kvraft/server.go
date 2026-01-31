package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Uuid  int
	Type  string
	Key   string
	Value string
}

type KVServer struct {
	sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	db           map[string]string
	waitingOps   map[int]chan Op // operations waiting to be commited by raft layer
	comittedHist map[int]bool    // dupe detection on retries from client layer
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.Lock()
	if kv.comittedHist[args.Uuid] {
		reply.Value = kv.db[args.Key] // already commited
		reply.Err = OK
		kv.Unlock()
		return
	}

	commandIndex, _, isLeader := kv.rf.Start(Op{args.Uuid, "Get", args.Key, ""})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.Unlock()
		return
	}

	ch := make(chan Op)
	kv.waitingOps[commandIndex] = ch
	kv.Unlock()

	commitedOp := <-ch
	if commitedOp.Uuid == args.Uuid {
		reply.Value = commitedOp.Value
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.putAppend(args, reply, "Put")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.putAppend(args, reply, "Append")
}

func (kv *KVServer) putAppend(args *PutAppendArgs, reply *PutAppendReply, opType string) {
	kv.Lock()
	if kv.comittedHist[args.Uuid] {
		reply.Err = OK // already commited
		kv.Unlock()
		return
	}

	commandIndex, _, isLeader := kv.rf.Start(Op{args.Uuid, opType, args.Key, args.Value})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.Unlock()
		return
	}

	ch := make(chan Op)
	kv.waitingOps[commandIndex] = ch
	kv.Unlock()

	commitedOp := <-ch
	if commitedOp.Uuid == args.Uuid {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) ApplyOperations() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.Lock()

			op := msg.Command.(Op)
			if !kv.comittedHist[op.Uuid] {
				if op.Type == "Put" {
					kv.db[op.Key] = op.Value
				}
				if op.Type == "Append" {
					kv.db[op.Key] += op.Value
				}

				kv.comittedHist[op.Uuid] = true
			}

			// we should still rspond to get commands even if dupe, to unblock waiting goroutines
			if op.Type == "Get" {
				op.Value, _ = kv.db[op.Key]
			}

			if ch, ok := kv.waitingOps[msg.CommandIndex]; ok {
				ch <- op
				delete(kv.waitingOps, msg.CommandIndex)
			}

			kv.Unlock()
		}
		// TODO: snapshots
	}
}

// The tester calls Kill() when a KVServer instance won't be needed again. For
// your convenience, we supply code to set rf.dead (without needing a lock), and
// a killed() method to test rf.dead in long-running loops. You can also add
// your own code to Kill(). You're not required to do anything about this, but
// it may be convenient (for example) to suppress debug output from a Kill()ed
// instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of servers that will cooperate via
// Raft to form the fault-tolerant key/value service. me is the index of the
// current server in servers[].
//
// The k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot. The k/v server should
// snapshot when Raft's saved state exceeds maxraftstate bytes, in order to
// allow Raft to garbage-collect its log. If maxraftstate is -1, you don't need
// to snapshot.
//
// StartKVServer() must return quickly, so it should start goroutines for any
// long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.waitingOps = make(map[int]chan Op)
	kv.comittedHist = make(map[int]bool)

	go kv.ApplyOperations() // waits for raft commits async

	return kv
}
