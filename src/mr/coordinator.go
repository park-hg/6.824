package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	MapTaskInfo    map[string]WorkerStatus
	ReduceTaskInfo map[int]WorkerStatus

	mu sync.Mutex
}

type WorkerStatus struct {
	ID        int
	Status    Status
	Locations []string
}

type Status string

const (
	Idle       = Status("idle")
	InProgress = Status("in-progress")
	Completed  = Status("completed")
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetMapTask(args *AskMapTaskArgs, reply *AskMapTaskReply) error {
	for filename, status := range c.MapTaskInfo {
		c.mu.Lock()
		if status.Status == Idle {
			reply.Filename = filename
			reply.Buckets = len(c.ReduceTaskInfo)

			c.MapTaskInfo[filename] = WorkerStatus{ID: args.WorkerID, Status: InProgress}
			c.mu.Unlock()
			break
		}
		c.mu.Unlock()
	}
	return nil
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, reply *CompleteMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.MapTaskInfo[args.Filename].Status == Completed {
		reply.Error = nil
		return nil
	}
	c.MapTaskInfo[args.Filename] = WorkerStatus{ID: args.WorkerID, Status: Completed, Locations: args.Locations}
	reply.Error = nil
	return nil
}

func (c *Coordinator) GetReduceTask(args *AskReduceTaskArgs, reply *AskReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for taskID := range c.ReduceTaskInfo {
		if c.ReduceTaskInfo[taskID].Status != Idle {
			continue
		}
		c.ReduceTaskInfo[taskID] = WorkerStatus{ID: args.WorkerID, Status: InProgress}
		reply.TaskID = taskID
		reply.Locations = c.makeIntermediateLocations(taskID)
		return nil
	}

	reply.TaskID = -1
	return nil
}

func (c *Coordinator) makeIntermediateLocations(reduceTaskID int) []string {
	locations := make([]string, len(c.MapTaskInfo))
	i := 0
	for _, v := range c.MapTaskInfo {
		locations[i] = v.Locations[reduceTaskID]
		i++
	}
	sort.Strings(locations)
	return locations
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *CompleteReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ReduceTaskInfo[args.TaskID] = WorkerStatus{ID: args.WorkerID, Status: Completed, Locations: args.Locations}
	reply.Error = nil
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
	c := Coordinator{}

	// Your code here.
	c.MapTaskInfo = make(map[string]WorkerStatus, len(files)-1)
	for _, f := range files[1:] {
		c.MapTaskInfo[f] = WorkerStatus{ID: -1, Status: Idle}
	}
	c.ReduceTaskInfo = make(map[int]WorkerStatus, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskInfo[i] = WorkerStatus{ID: -1, Status: Idle}
	}

	c.server()
	return &c
}
