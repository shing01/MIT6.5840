package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}
		ok := call("Coordinator.AskTask", &args, &reply)

		if !ok {
			fmt.Println("Worker: 联系不上 Coordinator")
			return
		}

		switch reply.TaskType {
		case "Map":
			fmt.Printf("Worker: 拿到 Map 任务 %d, 处理文件 %s\n", reply.TaskId, reply.Filename)
			time.Sleep(time.Second)
		
		case "Wait":
			fmt.Println("Worker: 等待分配任务")
			time.Sleep(time.Second)

		default:
			time.Sleep(time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := AskTaskArgs{}

	reply := AskTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Worker: 成功请求到任务! \n")
        fmt.Printf("  > 类型: %s\n", reply.TaskType)
        fmt.Printf("  > ID: %d\n", reply.TaskId)
        fmt.Printf("  > 文件: %s\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
