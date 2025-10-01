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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskRequestArgs struct {
	PreviousTaskID       int
	PreviousTaskType     TaskType
	PreviousTaskComplete bool
}

type TaskRequestReply struct {
	TaskID  int
	Type    TaskType
	Files   []string
	NReduce int
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Done
)

func (t TaskType) String() string {
	return [...]string{"Map", "Reduce", "Wait", "Done"}[t]
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
