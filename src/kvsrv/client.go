package kvsrv

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, "0"}
	reply := GetReply{}

	// send the RPC request, wait for the reply.
	ok := ck.server.Call("KVServer.Get", &args, &reply)

	if ok {
		return reply.Value
	} else {
		fmt.Printf("job request to server failed!\n")
		os.Exit(0)
	}

	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	reqId := nrand()
	args := PutAppendArgs{key, value, reqId}
	reply := PutAppendReply{}

	// send the RPC request, wait for the reply.
	ok := ck.server.Call("KVServer."+op, &args, &reply)

	if ok {
		return reply.Value
	} else {
		fmt.Printf("request to server failed!\n")
		os.Exit(0)
	}

	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
