package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	status      []int    //status of each job -- 0 in progress, 1 finished, -1 failure
	mapQueue    []string //filenames to map
	reduceQueue []bool   //reduce jobs to send out
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetNextJob(args *WorkerReqArgs, reply *CoordinatorReply) error {
	c.mu.Lock()

	if len(c.mapQueue) > 0 {
		//all maps must go first
		reply.TaskType = "MAP"
		reply.NReduce = len(c.reduceQueue)

		reply.Filename = c.mapQueue[0]
		c.mapQueue = c.mapQueue[1:]

	} else if slices.Contains(c.reduceQueue, false) {
		//we schedule reduce jobs
		reply.TaskType = "REDUCE"
		reply.NReduce = len(c.reduceQueue)

		for i := range c.reduceQueue {
			if !c.reduceQueue[i] {
				reply.TaskID = i
				c.reduceQueue[i] = true
				break
			}
		}
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReportJobStatus(args *WorkerReqArgs, reply *CoordinatorReply) error {
	c.mu.Lock()

	if args.Status != 1 {
		//job fail, add data back to the queues
		if args.Filename != "" {
			c.mapQueue = append(c.mapQueue, args.Filename)
		} else {
			c.reduceQueue[args.ReduceJob] = false
		}
	}

	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.status != nil {
		ret = true
		for _, job := range c.status {
			if job == 0 {
				return false
			}
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapQueue = files
	c.reduceQueue = make([]bool, nReduce)

	c.server()
	return &c
}
