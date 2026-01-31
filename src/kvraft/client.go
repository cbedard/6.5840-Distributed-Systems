package kvraft

import (
	"math/rand/v2"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	currentServer int
	sync.Mutex
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.currentServer = 0

	return ck
}

func (ck *Clerk) nextServer() {
	ck.Lock()
	defer ck.Unlock()

	ck.currentServer++
	ck.currentServer %= len(ck.servers)
}

// Fetch the current value for a key. Returns "" if the key does not exist.
// Keeps trying forever in the face of all other errors.
//
// You can send an RPC with code like this:
//
//	ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// The types of args and reply (including whether they are pointers) must match
// the declared types of the RPC handler function's arguments, and reply must be
// passed as a pointer.
func (ck *Clerk) Get(key string) string {
	uuid := int(rand.Int64())
	args := &GetArgs{uuid, key}

	for {
		server := ck.currentServer
		reply := &GetReply{}
		ok := callWithTimeout(func() bool {
			return ck.servers[server].Call("KVServer.Get", args, reply)
		}, 200*time.Millisecond)

		if ok && reply.Err == OK {
			return reply.Value
		}
		ck.nextServer()
	}
}

// Shared by Put and Append.
// The types of args and reply (including whether they are pointers) must match
// the declared types of the RPC handler function's arguments, and reply must be
// passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	uuid := int(rand.Int64())
	args := &PutAppendArgs{uuid, key, value}

	for {
		server := ck.currentServer
		reply := &PutAppendReply{}
		ok := callWithTimeout(func() bool {
			return ck.servers[server].Call("KVServer."+op, args, reply)
		}, 200*time.Millisecond)

		if ok && reply.Err == OK {
			return
		}
		ck.nextServer()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func callWithTimeout(callFn func() bool, timeout time.Duration) bool {
	done := make(chan bool, 1)
	go func() {
		done <- callFn()
	}()

	select {
	case ok := <-done:
		return ok
	case <-time.After(timeout):
		return false
	}
}
