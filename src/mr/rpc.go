package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
// 请求结构体
type AskTaskArgs struct {
	// WorkerId int
}

// 回复结构体
type AskTaskReply struct {
	TaskType string // "Map" "Reduce" "Wait" "Exit"
	TaskId int
	Filename string // 如果是Map任务，这里是文件名
	NReduce int 	// 总共有多少个Reduce任务
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
