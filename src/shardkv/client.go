package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"math/rand/v2"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// do not change this function (tester visibility)
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	make_end  func(string) *labrpc.ClientEnd
	clientId  int
	requestId int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientId = int(rand.Int64())
	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) nextRequestId() int {
	ck.requestId++
	return ck.requestId
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{ck.clientId, ck.nextRequestId(), key}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply

				ok := callWithTimeout(func() bool {
					return srv.Call("ShardKV.Get", args, &reply)
				}, 200*time.Millisecond)

				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{ck.clientId, ck.nextRequestId(), key, value}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply

				ok := callWithTimeout(func() bool {
					return srv.Call("ShardKV."+op, args, &reply)
				}, 200*time.Millisecond)

				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
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
