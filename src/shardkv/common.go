package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId  int
	RequestId int
	Key       string
	Value     string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId  int
	RequestId int
	Key       string
}

type GetReply struct {
	Err   Err
	Value string
}
