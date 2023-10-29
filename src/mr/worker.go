package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strings"
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

	// uncomment to send the Example RPC to the coordinator.
	filename, buckets := CallMapTaskAsk()
	if filename == "" {
		return
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	filetitle := strings.Split(filename, ".")[0]
	dir := fmt.Sprintf("m-intermediate-%s", filetitle)
	os.Mkdir(dir, 0777)

	intermediates := make([]*os.File, buckets)
	locations := make([]string, buckets)
	for i := 0; i < buckets; i++ {
		iname := fmt.Sprintf("%s/out-%d", dir, i)
		ofile, _ := os.Create(iname)
		intermediates[i] = ofile
		locations[i] = iname
	}

	for i := range kva {
		json.NewEncoder(intermediates[i%buckets]).Encode(&kva[i])
	}
	for _, f := range intermediates {
		f.Close()
	}

	CallMapTaskComplete(filename, locations)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallMapTaskAsk() (string, int) {
	args := AskMapTaskArgs{WorkerID: os.Getpid()}
	reply := AskMapTaskReply{}
	call("Coordinator.GetMapTask", &args, &reply)

	return reply.Filename, reply.Buckets
}

func CallMapTaskComplete(filename string, locations []string) error {
	args := CompleteMapTaskArgs{WorkerID: os.Getpid(), Filename: filename, Locations: locations}
	reply := CompleteMapTaskReply{}
	call("Coordinator.CompleteMapTask", &args, &reply)

	return reply.Error
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
