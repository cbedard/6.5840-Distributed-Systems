package shardctrler

//
// Shardctrler clerk.
//

import (
	"math/rand/v2"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int
	requestId int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:  servers,
		clientId: int(rand.Int64()),
	}
}

func (ck *Clerk) nextRequestId() int {
	ck.requestId++
	return ck.requestId
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{RequestHeader{ck.clientId, ck.nextRequestId()}, num}
	for {
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := callWithTimeout(func() bool {
				return srv.Call("ShardCtrler.Query", args, &reply)
			}, 200*time.Millisecond)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{RequestHeader{ck.clientId, ck.nextRequestId()}, servers}
	for {
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := callWithTimeout(func() bool {
				return srv.Call("ShardCtrler.Join", args, &reply)
			}, 200*time.Millisecond)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{RequestHeader{ck.clientId, ck.nextRequestId()}, gids}
	for {
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := callWithTimeout(func() bool {
				return srv.Call("ShardCtrler.Leave", args, &reply)
			}, 200*time.Millisecond)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{RequestHeader{ck.clientId, ck.nextRequestId()}, shard, gid}
	for {
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := callWithTimeout(func() bool {
				return srv.Call("ShardCtrler.Move", args, &reply)
			}, 200*time.Millisecond)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
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
