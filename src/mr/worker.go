package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/json"
import "os"
import "io/ioutil"
import "sort"


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

func doMap(mapf func(string, string) []KeyValue, filename string, mapId int, nReduce int) {
	// 1. read files
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 2. call mapf(plugin) to get key-value pairs
	kva := mapf(filename, string(content))

	// 3. nReduce buffers
	hashedKV := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		hashedKV[i] = []KeyValue{}
	}

	// 4. partitioning
	for _, kv := range kva {
		bucketId := ihash(kv.Key) % nReduce
		hashedKV[bucketId] = append(hashedKV[bucketId], kv)
	}

	// 5. write files
	for i := 0; i < nReduce; i++ {
		outputFilename := fmt.Sprintf("mr-%d-%d", mapId, i)

		outFile, _ := os.Create(outputFilename)

		// write by json
		enc := json.NewEncoder(outFile)
		for _, kv := range hashedKV[i] {
			enc.Encode(&kv)
		}
		outFile.Close()
	}
}

func doReduce(reducef func(string, []string) string, reduceId int, nMap int) {
	// read all intermediate files
	// file format: mr-X-Y --> X: MapId Y: current reduceId
	var intermediate []KeyValue

	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceId)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort --> combine all the same Key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// create output files mr-out-Y
	oname := fmt.Sprintf("mr-out-%d", reduceId)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[]
	// and print the result to mr-out-Y
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// Reduce output
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func CallReportTask(taskType string, taskId int) {
	args := ReportTaskArgs {
		TaskType: taskType,
		TaskId:   taskId,
	}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
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
			fmt.Println("Worker: cannot contact Coordinator")
			return
		}

		switch reply.TaskType {
		case "Map":
			fmt.Printf("Worker: get Map task %d, process file %s\n", reply.TaskId, reply.Filename)
			doMap(mapf, reply.Filename, reply.TaskId, reply.NReduce)
			CallReportTask("Map", reply.TaskId)
		
		case "Reduce":
			fmt.Printf("Worker: get Reduce task %d\n", reply.TaskId)
			doReduce(reducef, reply.TaskId, reply.NMap)
			CallReportTask("Reduce", reply.TaskId)

		case "Wait":
			fmt.Println("Worker: waiting for task")
			time.Sleep(time.Second)

		case "Exit":
			fmt.Println("Worker: all tasks are completed")
			return

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
