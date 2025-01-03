package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu         sync.Mutex
	m          map[string]string
	reqHistory map[int64]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	val := kv.m[args.Key]

	reply.Value = val

	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.reqHistory[args.ReqId] {
		return
	}

	kv.m[args.Key] = args.Value
	kv.reqHistory[args.ReqId] = true

	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.reqHistory[args.ReqId] {
		return
	}

	reply.Value = kv.m[args.Key]
	kv.m[args.Key] += args.Value
	kv.reqHistory[args.ReqId] = true

	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := KVServer{}
	kv.m = map[string]string{}
	kv.reqHistory = map[int64]bool{}

	return &kv
}
