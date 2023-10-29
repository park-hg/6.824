package mr

import (
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
	WorkerInfo map[int]*WorkerInfo

	MapTaskInfo    map[int]*TaskInfo
	ReduceTaskInfo map[int]*TaskInfo

	mu sync.Mutex
}

type WorkerInfo struct {
	TaskID int
	Status WorkerStatus
}

type TaskInfo struct {
	Finished bool
	Inputs   []string
	Outputs  []string
}

type WorkerStatus string

const (
	Idle    = WorkerStatus("idle")
	Running = WorkerStatus("running")
	Crashed = WorkerStatus("crashed")
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type HandShakeArgs struct {
	WorkerID int
}

type HandShakeReply struct {
	Status string
}

func (c *Coordinator) HandShake(args *HandShakeArgs, reply *HandShakeReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.WorkerInfo[args.WorkerID] = &WorkerInfo{Status: Idle}
	reply.Status = "registered"
	return nil
}

func (c *Coordinator) GetMapTask(args *AskMapTaskArgs, reply *AskMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, task := range c.MapTaskInfo {
		if task.Finished {
			continue
		}
		c.WorkerInfo[args.WorkerID].TaskID = id
		c.WorkerInfo[args.WorkerID].Status = Running

		reply.Filename = task.Inputs[0]
		reply.Buckets = len(c.ReduceTaskInfo)
		break
	}

	return nil
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, reply *CompleteMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.MapTaskInfo[c.WorkerInfo[args.WorkerID].TaskID].Finished {
		c.WorkerInfo[args.WorkerID].Status = Idle
		reply.Error = nil
		return nil
	}

	c.MapTaskInfo[c.WorkerInfo[args.WorkerID].TaskID].Finished = true
	c.MapTaskInfo[c.WorkerInfo[args.WorkerID].TaskID].Outputs = args.Outputs

	if len(args.Outputs) != len(c.ReduceTaskInfo) {
		panic("outputs of map worker should have the same length with nReduce")
	}

	for i, output := range args.Outputs {
		c.ReduceTaskInfo[i].Inputs = append(c.ReduceTaskInfo[i].Inputs, output)
	}

	c.WorkerInfo[args.WorkerID].Status = Idle
	reply.Error = nil
	return nil
}

func (c *Coordinator) GetReduceTask(args *AskReduceTaskArgs, reply *AskReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, task := range c.ReduceTaskInfo {
		if task.Finished {
			continue
		}
		c.WorkerInfo[args.WorkerID].TaskID = id
		c.WorkerInfo[args.WorkerID].Status = Running
		reply.TaskID = id
		reply.Inputs = task.Inputs
		return nil
	}

	reply.TaskID = -1
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *CompleteReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ReduceTaskInfo[args.TaskID].Finished = true
	c.ReduceTaskInfo[args.TaskID].Outputs = args.Locations

	c.WorkerInfo[args.WorkerID].Status = Idle
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
	time.Sleep(60 * time.Second)
	ret = true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.WorkerInfo = make(map[int]*WorkerInfo)

	c.MapTaskInfo = make(map[int]*TaskInfo, len(files)-1)
	for i, f := range files[1:] {
		c.MapTaskInfo[i] = &TaskInfo{Inputs: []string{f}, Finished: false}
	}

	c.ReduceTaskInfo = make(map[int]*TaskInfo, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskInfo[i] = &TaskInfo{Finished: false, Inputs: make([]string, nReduce)}
	}

	c.server()
	return &c
}
