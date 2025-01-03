package kvsrv

import (
	"os"
	"strconv"
)

// Field names must start with capital letters,
// otherwise RPC will break.

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	ReqId string
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

func serverSock() string {
	s := "/var/tmp/5840-kvsrv-"
	s += strconv.Itoa(os.Getuid())
	return s
}
