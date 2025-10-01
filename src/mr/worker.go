package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
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
	var previousTaskType TaskType
	var previousTaskID int
	var previousTaskComplete bool
OuterLoop:
	for {
		args := TaskRequestArgs{
			PreviousTaskID:       previousTaskID,
			PreviousTaskType:     previousTaskType,
			PreviousTaskComplete: previousTaskComplete,
		}
		reply := TaskRequestReply{}

		ok := call("Coordinator.TaskRequest", &args, &reply)
		if ok {
			fmt.Printf("reply %v\n", reply)
		} else {
			fmt.Printf("call failed!\n")
			time.Sleep(time.Second)
			previousTaskID = 0
			previousTaskType = Map
			previousTaskComplete = false
			continue
		}

		previousTaskID = reply.TaskID
		previousTaskType = reply.Type
		previousTaskComplete = false

		switch reply.Type {
		case Map:
			ExecuteMapTask(mapf, reply)
			previousTaskComplete = true
		case Reduce:
			ExecuteReduceTask(reducef, reply)
			previousTaskComplete = true
		case Wait:
			time.Sleep(time.Second)
		case Done:
			break OuterLoop
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func ExecuteMapTask(mapf func(string, string) []KeyValue,
	reply TaskRequestReply) {

	// Read input from file
	inputFilePath := reply.Files[0]
	file, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("cannot open %v", inputFilePath)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFilePath)
	}

	// Compute map function on input
	mapOutput := []KeyValue{}
	kva := mapf(inputFilePath, string(content))
	mapOutput = append(mapOutput, kva...)

	// Write map output to nReduce buckets
	outFiles := make([]*os.File, reply.NReduce)
	enc := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		outFileName := fmt.Sprintf("out-%v-%v-tmp", reply.TaskID, i)
		outFile, err := os.CreateTemp("", outFileName)
		if err != nil {
			log.Fatalf("cannot open %v", outFileName)
		}
		defer outFile.Close()
		outFiles[i] = outFile
		enc[i] = json.NewEncoder(outFile)
	}

	for _, kv := range mapOutput {
		bucketNumber := ihash(kv.Key) % reply.NReduce
		err := enc[bucketNumber].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write %v from file %v", kv, inputFilePath)
		}
	}

	for bucketNumber, outFile := range outFiles {
		oldName := outFile.Name()
		newName := fmt.Sprintf("mr-map-out-%v-%v", reply.TaskID, bucketNumber)
		err := os.Rename(oldName, newName)
		if err != nil {
			log.Fatalf("cannot rename %v to %v", oldName, newName)
		}
	}
}

func ExecuteReduceTask(reducef func(string, []string) string,
	reply TaskRequestReply) {

	// Open files containing output from the map
	reduceInput := make(map[string][]string)
	files := reply.Files

	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		// Collect key-value pairs by key
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			reduceInput[kv.Key] = append(reduceInput[kv.Key], kv.Value)
		}
	}

	oname := fmt.Sprintf("mr-out-%v-%v", reply.NReduce, reply.TaskID)
	ofile, _ := os.CreateTemp("", oname)
	defer ofile.Close()

	for key, values := range reduceInput {
		output := reducef(key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	newName := fmt.Sprintf("mr-out-%v", reply.NReduce)
	err := os.Rename(ofile.Name(), newName)
	if err != nil {
		log.Fatalf("cannot rename %v to %v", ofile.Name(), newName)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
