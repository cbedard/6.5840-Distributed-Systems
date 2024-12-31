package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var nReduce = 10

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
	retries := 0

	for {
		//TODO make this a goroutine loop so worker stays running and receives task
		response := ReadyToWork()
		errRes := CoordinatorReply{}

		//no job right now, try again in a sec
		if response == errRes {
			if retries < 3 {
				time.Sleep(time.Second * 2)
				retries++
				continue
			} else {
				break
			}
		}

		//jobs are complete!!
		if response.TaskID == -1 {
			break
		}

		//nReduce = response.NReduce
		if response.TaskType == "MAP" {
			ok := HandleMap(response.TaskID, response.Filename, mapf)

			ReportJobStatus(ok, response.Filename, response.TaskID)
		}

		if response.TaskType == "REDUCE" {
			ok := HandleReduce(response.TaskID, reducef)

			ReportJobStatus(ok, response.Filename, response.TaskID)
		}
	}
	fmt.Println("worker-node exiting.")
}

func HandleMap(id int, filename string, mapf func(string, string) []KeyValue) bool {
	content := ReadFileAsByteArr(filename)

	kva := mapf(filename, string(content))

	WriteIntermediateFiles(id, kva)
	return true
}

func HandleReduce(reduceNumber int, reducef func(string, []string) string) bool {
	//read intermediate file and split
	filename := "mr-intermediate-" + strconv.Itoa(reduceNumber)
	rawInput := string(ReadIntermediateFiles(strconv.Itoa(reduceNumber)))
	splitInput := strings.Split(rawInput, "\n")

	intermediate := make([]KeyValue, 0)
	for _, line := range splitInput {
		tmp := strings.Split(line, " ")

		if len(tmp) == 2 {
			k := tmp[0]
			v := tmp[1]

			intermediate = append(intermediate, KeyValue{k, v})
		}
	}

	//create output file
	file, err := os.Create("mr-out-" + strconv.Itoa(reduceNumber))
	if err != nil {
		log.Fatalf("cannot open %v, file probably already exists", filename)
	}

	slices.SortFunc(intermediate, func(i, j KeyValue) int {
		return strings.Compare(i.Key, j.Key)
	})

	//group keys and append to output file
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	file.Close()
	return true
}

func ReadFileAsByteArr(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return content
}

func WriteIntermediateFiles(mapId int, content []KeyValue) {
	//we need new files for each reduce option, key val using ihash
	fileOutput := make([]string, nReduce)
	for _, line := range content {
		index := ihash(line.Key) % nReduce

		fileOutput[index] += (line.Key + " " + line.Value + "\n")
	}

	for reduceN, line := range fileOutput {
		filename := "mr-intermediate-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reduceN)

		err := os.WriteFile(filename, []byte(line), 0644)
		if err != nil {
			fmt.Println("intermediate-file write error", err)
		}

		//TODO: handle errors with retry?
	}
}

func ReadIntermediateFiles(reduceID string) []byte {
	matches, _ := filepath.Glob("mr-intermediate-*-" + reduceID)

	res := make([]byte, 0)
	for _, path := range matches {
		res = append(res, ReadFileAsByteArr(path)...)
	}

	return res
}

func WriteOutputFile(filename string, content []KeyValue) {
	ofile, _ := os.Create(filename)

	for i := range content {
		fmt.Fprintf(ofile, "%v %v\n", content[i].Key, content[i].Value)
	}

	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func ReadyToWork() CoordinatorReply {
	// declare an argument structure.
	args := WorkerReqArgs{}

	// fill in the argument(s).
	args.Status = 0

	// declare a reply structure.
	reply := CoordinatorReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetNextJob", &args, &reply)

	if ok {
		return reply
	} else {
		fmt.Printf("job request to coordinator failed!\n")
		os.Exit(0)
	}
	return CoordinatorReply{}
}

func ReportJobStatus(status bool, filename string, id int) {
	// declare an argument structure.
	args := WorkerReqArgs{}
	args.Status = 1
	args.Filename = filename
	args.ReduceJob = id

	if !status {
		args.Status = -1
	}

	reply := CoordinatorReply{}
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.ReportJobStatus", &args, &reply)

	if !ok {
		fmt.Printf("Report call to coordinator failed!\n")
		os.Exit(0)
	}
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
