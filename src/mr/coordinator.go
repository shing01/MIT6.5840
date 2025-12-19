package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"

const (
	TaskStatusIdle       = 0
	TaskStatusInProgress = 1
	TaskStatusCompleted  = 2
)

type Task struct {
	Type      string   // "Map" "Reduce"
	Status    int	   // "Idle" "InProgress" "Completed"
	Index     int	   // Map task --> file index, Reduce task --> hash bucket number
	Filename  string
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
	mu          sync.Mutex

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
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := true

	// Your code here.
	for _, task := range c.mapTasks {
		if task.Status != TaskStatusCompleted {
			ret = false
			return ret
		}
	}

	for _, task := range c.reduceTasks {
		if task.Status != TaskStatusCompleted {
			ret = false
			return ret
		}
	}

	return ret
}

// RPC Handler
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mapDone := true 
	for i, task := range c.mapTasks {
		// check if all Map tasks are completed
		if task.Status != TaskStatusCompleted {
			mapDone = false
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
	}

	// Map tasks not all completed and no Idle tasks
	if !mapDone {
		reply.TaskType = "Wait"
		return nil
	}

	// mapDone == true --> reduce stage
	reduceDone := true
	for i, task := range c.reduceTasks {
		if task.Status != TaskStatusCompleted {
			reduceDone = false
			if task.Status == TaskStatusIdle {
				c.reduceTasks[i].Status = TaskStatusInProgress
				c.reduceTasks[i].StartTime = time.Now()

				reply.TaskType = "Reduce"
				reply.TaskId = task.Index
				reply.NMap = len(c.files)

				return nil
			}
		}
	}

	if !reduceDone {
		reply.TaskType = "Wait"
	} else {
		reply.TaskType = "Exit"
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "Map" {
		c.mapTasks[args.TaskId].Status = TaskStatusCompleted
		fmt.Printf("Coordinator: Map task %d is completed\n", args.TaskId)
	} else if args.TaskType == "Reduce" {
		c.reduceTasks[args.TaskId].Status = TaskStatusCompleted
		fmt.Printf("Coordinator: Reduce task %d is completed\n", args.TaskId)
	}

	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:	     files,
		nReduce:  	 nReduce,
		mapTasks: 	 make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	// Your code here.
	// init Map task
	for i, file := range files {
		c.mapTasks[i] = Task{
			Type:     "Map",
			Status:   TaskStatusIdle,
			Index:    i,
			Filename: file,
		}
	}

	// init Reduce task
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type:	 "Reduce",
			Status:	 TaskStatusIdle,
			Index:	 i,
		}
	}

	c.server()

	// monitor goroutine
	go func() {
		for {
			// check per second
			time.Sleep(time.Second)
			c.mu.Lock()

			// check Map task
			for i := 0; i < len(c.mapTasks); i++ {
				if c.mapTasks[i].Status == TaskStatusInProgress &&
					time.Since(c.mapTasks[i].StartTime) > 10*time.Second {
						c.mapTasks[i].Status = TaskStatusIdle
						fmt.Printf("Coordinator: Map task %d Timeout, reset\n", i)
				}
			}

			// check Reduce task
			for i := 0; i < len(c.reduceTasks); i++ {
				if c.reduceTasks[i].Status == TaskStatusInProgress &&
					time.Since(c.reduceTasks[i].StartTime) > 10*time.Second {
						c.reduceTasks[i].Status = TaskStatusIdle
						fmt.Printf("Coordinator: Reduce task %d Timeout, reset\n", i)
				}
			}
			c.mu.Unlock()
		}
	}()

	return &c
}
