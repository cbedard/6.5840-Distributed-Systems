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
	reqHistory map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	val := kv.m[args.Key]
	reply.Value = val

	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	old, ok := kv.reqHistory[args.ReqId]

	if !ok {
		kv.m[args.Key] = args.Value
		kv.reqHistory[args.ReqId] = args.Value
	} else {
		reply.Value = old
	}

	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	old, ok := kv.reqHistory[args.ReqId]

	if !ok {

		reply.Value = kv.m[args.Key]
		kv.reqHistory[args.ReqId] = reply.Value

		kv.m[args.Key] += args.Value
	} else {
		reply.Value = old
	}

	kv.mu.Unlock()
}

func (kv *KVServer) DeleteHistory(args *ConfirmArgs, reply *ConfirmReply) {
	kv.mu.Lock()
	delete(kv.reqHistory, args.ReqId)
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := KVServer{}
	kv.m = map[string]string{}
	kv.reqHistory = map[int64]string{}

	return &kv
}
