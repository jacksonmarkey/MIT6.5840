package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files                []string
	nReduce              int
	mu                   sync.Mutex
	taskIDCounter        int
	inProgress           map[int]struct{}
	completedMapTasks    map[int]struct{}
	completedReduceTasks map[int]struct{}
	mapTaskQueue         []string
	reduceTaskQueue      []int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	// fmt.Printf("Recieved: %v\n", args)
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.PreviousTaskComplete {
		if _, ok := c.inProgress[args.PreviousTaskID]; ok {
			delete(c.inProgress, args.PreviousTaskID)
			switch args.PreviousTaskType {
			case Map:
				c.completedMapTasks[args.PreviousTaskID] = struct{}{}
			case Reduce:
				c.completedReduceTasks[args.PreviousTaskID] = struct{}{}
			}
		}
	}

	switch {
	case len(c.mapTaskQueue) > 0:
		filePath := c.mapTaskQueue[0]
		taskID := c.taskIDCounter
		reply.TaskID = taskID
		c.mapTaskQueue = c.mapTaskQueue[1:]
		reply.Type = Map
		c.inProgress[taskID] = struct{}{}
		c.taskIDCounter += 1
		reply.Files = []string{filePath}
		reply.NReduce = c.nReduce
		go func() {
			time.Sleep(10 * time.Second)
			c.mu.Lock()
			if _, ok := c.inProgress[taskID]; ok {
				delete(c.inProgress, taskID)
				c.mapTaskQueue = append(c.mapTaskQueue, filePath)
			}
			c.mu.Unlock()
		}()
	case len(c.completedMapTasks) < len(c.files):
		reply.Type = Wait
	case len(c.reduceTaskQueue) > 0:
		reply.Type = Reduce
		bucketNumber := c.reduceTaskQueue[0]
		reply.NReduce = bucketNumber
		taskID := c.taskIDCounter
		reply.TaskID = taskID
		c.inProgress[taskID] = struct{}{}
		c.reduceTaskQueue = c.reduceTaskQueue[1:]
		c.taskIDCounter += 1
		reply.Files = make([]string, len(c.files))
		i := 0
		for mapTaskID := range c.completedMapTasks {
			intermediateFileName := fmt.Sprintf("mr-map-out-%v-%v", mapTaskID, bucketNumber)
			reply.Files[i] = intermediateFileName
			i++
		}
		go func() {
			time.Sleep(10 * time.Second)
			c.mu.Lock()
			if _, ok := c.inProgress[taskID]; ok {
				delete(c.inProgress, taskID)
				c.reduceTaskQueue = append(c.reduceTaskQueue, bucketNumber)
			}
			c.mu.Unlock()
		}()
	case len(c.completedReduceTasks) < c.nReduce:
		reply.Type = Wait
	default:
		reply.Type = Done
	}
	// fmt.Printf("Sent: %v\n", reply)
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
	ret = (len(c.completedReduceTasks) == c.nReduce)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	mapTaskQueue := make([]string, len(files))
	copy(mapTaskQueue, files)
	reduceTaskQueue := make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTaskQueue[i] = i
	}
	c := Coordinator{
		files,
		nReduce,
		sync.Mutex{},
		1,
		make(map[int]struct{}),
		make(map[int]struct{}),
		make(map[int]struct{}),
		mapTaskQueue,
		reduceTaskQueue,
	}

	c.server()
	return &c
}
