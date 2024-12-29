package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	response := ReadyToWork()
	errRes := CoordinatorReply{}

	if response == errRes {
		return
	}

	if response.TaskType == "MAP" {
		HandleMap(response.TaskID, response.Filename, mapf)
	}

	if response.TaskType == "REDUCE" {
		HandleReduce()
	}
}

func HandleMap(id int, filename string, mapf func(string, string) []KeyValue) {
	content := ReadFileAsByteArr(filename)

	kva := mapf(filename, string(content))
	oFileName := "mr-" + strconv.Itoa(id) + " -"

	WriteIntermediateFiles(oFileName, kva)
}

func HandleReduce() {

}

func ReadFileAsByteArr(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	file.Close()
	return content
}

func WriteIntermediateFiles(filename string, content []KeyValue) {
	//we need new files for each key val using ihash
	ofile, _ := os.Create(filename)

	enc := json.NewEncoder(ofile)
	for _, kv := range content {
		enc.Encode(&kv)
	}

	ofile.Close()
}

func ReadIntermediateFile(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	kva := make([]KeyValue, 0)

	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	return kva
}

func WriteOutputFile(filename string, content []KeyValue) {
	ofile, _ := os.Create(filename)

	for i := range content {
		fmt.Fprintf(ofile, "%v %v\n", content[i].Key, content[i].Value)
	}

	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func ReadyToWork() CoordinatorReply {
	// declare an argument structure.
	args := WorkerReqArgs{}

	// fill in the argument(s).
	args.Status = 0

	// declare a reply structure.
	reply := CoordinatorReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply %v %v %v\n", reply.TaskID, reply.TaskType, reply.Filename)

		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return CoordinatorReply{}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
