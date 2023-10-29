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
type AskMapTaskArgs struct {
	WorkerID int
}

type AskMapTaskReply struct {
	Filename string
	Buckets  int
}

type CompleteMapTaskArgs struct {
	WorkerID int
	Filename string
	Outputs  []string
}

type CompleteMapTaskReply struct {
	Error error
}

type AskReduceTaskArgs struct {
	WorkerID int
}

type AskReduceTaskReply struct {
	TaskID int
	Inputs []string
}

type CompleteReduceTaskArgs struct {
	WorkerID  int
	TaskID    int
	Locations []string
}

type CompleteReduceTaskReply struct {
	Error error
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
