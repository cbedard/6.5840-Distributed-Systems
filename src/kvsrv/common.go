package kvsrv

// Field names must start with capital letters,
// otherwise RPC will break.

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	ReqId int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key   string
	ReqId string
}

type GetReply struct {
	Value string
}

type ConfirmArgs struct {
	ReqId int64
}

type ConfirmReply struct {
	Ok bool
}
