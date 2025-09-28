package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	fmt.Println("Coordinator: TaskRequest recieved successfully!")
	reply.IsMapTask = true
	reply.NReduce = c.nReduce
	// TODO: For each TaskRequest, pick a file from c.files, associate it with this worker
	// in a concurrency-safe map, along with it's start time?
	reply.FileName = c.files[0]
	// Gonna need to use channels here to communicate between workers and coordinator
	// Should it be owned/instantiated by the Coordinator or the Worker? hmm...
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files,
		nReduce,
	}

	// Your code here.

	c.server()
	return &c
}
