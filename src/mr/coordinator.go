package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
// import "fmt"
import "sync"
import "time"

const (
	TaskStatusIdle       = 0 // 任务闲置
	TaskStatusInProgress = 1 // 任务进行中
	TaskStatusCompleted  = 2 // 任务完成
)

type Task struct {
	Type      string   // "Map" "Reduce"
	Status    int	   // "Idle" "InProgress" "Completed"
	Index     int	   // 任务编号: Map任务对应文件下标, Reduce任务对应哈希桶编号
	Filename  string
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	files     []string
	nReduce   int
	mapTasks  []Task
	mu        sync.Mutex

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

// RPC Handler: 处理Worker的请求
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, task := range c.mapTasks {
		if task.Status == TaskStatusIdle {
			c.mapTasks[i].Status = TaskStatusInProgress
			c.mapTasks[i].StartTime = time.Now()

			reply.TaskType = "Map"
			reply.TaskId = task.Index
			reply.Filename = task.Filename
			reply.NReduce = c.nReduce

			return nil
		}
	}
	reply.TaskType = "Wait"

	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:	  files,
		nReduce:  nReduce,
		mapTasks: make([]Task, len(files)),
	}

	// Your code here.
	// 初始化Map任务
	for i, file := range files {
		c.mapTasks[i] = Task{
			Type:     "Map",
			Status:   TaskStatusIdle,
			Index:    i,
			Filename: file,
		}
	}


	c.server()
	return &c
}
