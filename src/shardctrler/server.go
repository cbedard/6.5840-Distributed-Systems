package shardctrler

import (
	"maps"
	"sort"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs    []Config        // indexed by config num
	waitingOps map[int]chan Op // commandIndex -> channel
	processed  map[int]int     // clientId -> last processed requestId
}

type Op struct {
	ClientId  int
	RequestId int
	Type      string // "Join", "Leave", "Move", "Query"

	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num    int
	Config Config // result for Query
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.Lock()
	if args.RequestId <= sc.processed[args.ClientId] {
		sc.Unlock()
		return
	}

	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, Type: "Join", Servers: args.Servers}
	commandIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.Unlock()
		return
	}

	ch := make(chan Op)
	sc.waitingOps[commandIndex] = ch
	sc.Unlock()

	committedOp := <-ch
	if committedOp.ClientId != args.ClientId || committedOp.RequestId != args.RequestId {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.Lock()
	if args.RequestId <= sc.processed[args.ClientId] {
		sc.Unlock()
		return
	}

	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, Type: "Leave", GIDs: args.GIDs}
	commandIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.Unlock()
		return
	}

	ch := make(chan Op)
	sc.waitingOps[commandIndex] = ch
	sc.Unlock()

	committedOp := <-ch
	if committedOp.ClientId != args.ClientId || committedOp.RequestId != args.RequestId {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.Lock()
	if args.RequestId <= sc.processed[args.ClientId] {
		sc.Unlock()
		return
	}

	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, Type: "Move", Shard: args.Shard, GID: args.GID}
	commandIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.Unlock()
		return
	}

	ch := make(chan Op)
	sc.waitingOps[commandIndex] = ch
	sc.Unlock()

	committedOp := <-ch
	if committedOp.ClientId != args.ClientId || committedOp.RequestId != args.RequestId {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.Lock()
	if args.RequestId <= sc.processed[args.ClientId] {
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.latestConfig()
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.Unlock()
		return
	}

	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, Type: "Query", Num: args.Num}
	commandIndex, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.Unlock()
		return
	}

	ch := make(chan Op)
	sc.waitingOps[commandIndex] = ch
	sc.Unlock()

	committedOp := <-ch
	if committedOp.ClientId == args.ClientId && committedOp.RequestId == args.RequestId {
		reply.Config = committedOp.Config
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) ApplyOperations() {
	for msg := range sc.applyCh {
		if !msg.CommandValid {
			continue
		}

		sc.Lock()
		op := msg.Command.(Op)

		if op.Type != "Query" && op.RequestId > sc.processed[op.ClientId] {
			newConfig := copyConfig(sc.latestConfig())
			newConfig.Num++

			switch op.Type {
			case "Join":
				maps.Copy(newConfig.Groups, op.Servers)
				sc.rebalance(&newConfig)
			case "Leave":
				for _, gid := range op.GIDs {
					delete(newConfig.Groups, gid)
				}
				sc.rebalance(&newConfig)
			case "Move":
				newConfig.Shards[op.Shard] = op.GID
			}

			sc.configs = append(sc.configs, newConfig)
			sc.processed[op.ClientId] = op.RequestId
		}

		// Query always returns current state (even if already processed)
		if op.Type == "Query" {
			if op.Num < 0 || op.Num >= len(sc.configs) {
				op.Config = sc.latestConfig()
			} else {
				op.Config = sc.configs[op.Num]
			}
		}

		if ch, ok := sc.waitingOps[msg.CommandIndex]; ok {
			ch <- op
			delete(sc.waitingOps, msg.CommandIndex)
		}

		sc.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.waitingOps = make(map[int]chan Op)
	sc.processed = make(map[int]int)

	go sc.ApplyOperations()

	return sc
}

func (sc *ShardCtrler) latestConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) rebalance(config *Config) {
	var gids []int
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	if len(gids) == 0 {
		config.Shards = [NShards]int{}
		return
	}

	// Assign orphaned shards to group with fewest
	for shard := range config.Shards {
		if _, ok := config.Groups[config.Shards[shard]]; ok {
			continue
		}
		minGid, minCount := gids[0], NShards+1
		counts := make(map[int]int)
		for _, g := range config.Shards {
			counts[g]++
		}
		for _, gid := range gids {
			if counts[gid] < minCount {
				minGid, minCount = gid, counts[gid]
			}
		}
		config.Shards[shard] = minGid
	}

	// Rebalance: move from max to min until balanced
	for {
		counts := make(map[int]int)
		for _, gid := range config.Shards {
			counts[gid]++
		}

		minGid, maxGid := gids[0], gids[0]
		for _, gid := range gids {
			if counts[gid] < counts[minGid] {
				minGid = gid
			}
			if counts[gid] > counts[maxGid] {
				maxGid = gid
			}
		}

		if counts[maxGid]-counts[minGid] <= 1 {
			break
		}

		for shard, gid := range config.Shards {
			if gid == maxGid {
				config.Shards[shard] = minGid
				break
			}
		}
	}
}
