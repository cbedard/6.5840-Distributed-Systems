package kvsrv

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	mu sync.Mutex

	// Your definitions here.
	m map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	val := kv.m[args.Key]

	reply.Value = val

	kv.mu.Unlock()
	return nil
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	kv.m[args.Key] = args.Value

	kv.mu.Unlock()
	return nil
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	reply.Value = kv.m[args.Key]
	kv.m[args.Key] += args.Value

	kv.mu.Unlock()
	return nil
}

func (kv *KVServer) server() {
	rpc.Register(kv)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := serverSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

func StartKVServer() *KVServer {
	kv := KVServer{}
	kv.m = make(map[string]string)

	kv.server()
	return &kv
}
