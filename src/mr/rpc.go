package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskExit
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	TaskType TaskType
	TaskID   int
	FileName string
	NMap     int
	NReduce  int
}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskID   int
}

type ReportTaskReply struct{}
