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
OuterLoop:
	for {
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}

		ok := call("Coordinator.TaskRequest", &args, &reply)
		if ok {
			fmt.Printf("reply %v\n", reply)
		} else {
			fmt.Printf("call failed!\n")
			time.Sleep(time.Second)
		}
		switch reply.Type {
		case Map:
			ExecuteMapTask(mapf, reply)
		case Reduce:
			ExecuteReduceTask(reducef, reply)
		case Done:
			break OuterLoop
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func ExecuteMapTask(mapf func(string, string) []KeyValue,
	reply TaskRequestReply) {

	intermediate := []KeyValue{}
	fileName := reply.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	defer file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	enc := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		outFileName := fmt.Sprintf("out-%v-%v", fileName, i)
		outFile, err := os.Create(outFileName)
		if err != nil {
			log.Fatalf("cannot open %v", outFileName)
		}
		defer outFile.Close()
		enc[i] = json.NewEncoder(outFile)
	}

	for _, kv := range intermediate {
		outTaskNumber := ihash(kv.Key) % reply.NReduce
		err := enc[outTaskNumber].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write %v from file %v", kv, fileName)
		}
	}
}

func ExecuteReduceTask(reducef func(string, []string) string,
	reply TaskRequestReply) {

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
