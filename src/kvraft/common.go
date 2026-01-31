package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type CommonReply struct {
	Err        Err
	LeaderHint int
}

func (r *CommonReply) GetErr() Err        { return r.Err }
func (r *CommonReply) GetLeaderHint() int { return r.LeaderHint }

type Reply interface {
	GetErr() Err
	GetLeaderHint() int
}

// Put or Append
type PutAppendArgs struct {
	Uuid  int
	Key   string
	Value string
}

type PutAppendReply struct {
	CommonReply
}

type GetArgs struct {
	Uuid int
	Key  string
}

type GetReply struct {
	CommonReply
	Value string
}
