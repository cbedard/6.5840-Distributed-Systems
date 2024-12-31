package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type WorkerReqArgs struct {
	Status    int    //READY: 0, DONE: 1, FAILED: -1
	Filename  string //only used if status = 1/-1 and we are reporting a map result
	ReduceJob int    //only used if status = 1/-1 and we are reporting a reduce result
}

type CoordinatorReply struct {
	TaskID   int
	TaskType string
	Filename string
	NReduce  int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
