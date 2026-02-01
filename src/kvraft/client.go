package kvraft

import (
	"math/rand/v2"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	currentServer int
	clientId      int
	requestId     int
	// sync.Mutex, the tester only over calls methods in client.go sequentially after receving an OK
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = int(rand.Int64())
	return ck
}

func (ck *Clerk) nextServer() int {
	ck.currentServer++
	ck.currentServer %= len(ck.servers)
	return ck.currentServer
}

func (ck *Clerk) nextRequestId() int {
	ck.requestId++
	return ck.requestId
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
	args := &GetArgs{ck.clientId, ck.nextRequestId(), key}

	for {
		server := ck.nextServer()
		reply := &GetReply{}

		ok := callWithTimeout(func() bool {
			return ck.servers[server].Call("KVServer.Get", args, reply)
		}, 200*time.Millisecond)

		if ok && reply.Err == OK {
			return reply.Value
		}
	}
}

// Shared by Put and Append.
// The types of args and reply (including whether they are pointers) must match
// the declared types of the RPC handler function's arguments, and reply must be
// passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{ck.clientId, ck.nextRequestId(), key, value}

	for {
		server := ck.nextServer()
		reply := &PutAppendReply{}

		ok := callWithTimeout(func() bool {
			return ck.servers[server].Call("KVServer."+op, args, reply)
		}, 200*time.Millisecond)

		if ok && reply.Err == OK {
			return
		}
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
