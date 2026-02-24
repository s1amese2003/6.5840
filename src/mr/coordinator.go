package mr

import "log"
import "net"
import "os"
import "net/http"
import "net/rpc"
import "sync"
import "time"

type taskState int

const (
	taskIdle taskState = iota
	taskRunning
	taskDone
)

type taskInfo struct {
	fileName  string
	state     taskState
	startTime time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []taskInfo
	reduceTasks []taskInfo
	timeout     time.Duration
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.allMapDoneLocked() {
		if taskID, ok := c.pickMapTaskLocked(); ok {
			reply.TaskType = TaskMap
			reply.TaskID = taskID
			reply.FileName = c.mapTasks[taskID].fileName
			reply.NReduce = len(c.reduceTasks)
		} else {
			reply.TaskType = TaskWait
		}
		return nil
	}

	if !c.allReduceDoneLocked() {
		if taskID, ok := c.pickReduceTaskLocked(); ok {
			reply.TaskType = TaskReduce
			reply.TaskID = taskID
			reply.NMap = len(c.mapTasks)
		} else {
			reply.TaskType = TaskWait
		}
		return nil
	}

	reply.TaskType = TaskExit
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case TaskMap:
		if args.TaskID >= 0 && args.TaskID < len(c.mapTasks) {
			if c.mapTasks[args.TaskID].state == taskRunning {
				c.mapTasks[args.TaskID].state = taskDone
			}
		}
	case TaskReduce:
		if args.TaskID >= 0 && args.TaskID < len(c.reduceTasks) {
			if c.reduceTasks[args.TaskID].state == taskRunning {
				c.reduceTasks[args.TaskID].state = taskDone
			}
		}
	}
	return nil
}

func (c *Coordinator) pickMapTaskLocked() (int, bool) {
	for i := range c.mapTasks {
		if c.canAssignTaskLocked(&c.mapTasks[i]) {
			c.mapTasks[i].state = taskRunning
			c.mapTasks[i].startTime = time.Now()
			return i, true
		}
	}
	return -1, false
}

func (c *Coordinator) pickReduceTaskLocked() (int, bool) {
	for i := range c.reduceTasks {
		if c.canAssignTaskLocked(&c.reduceTasks[i]) {
			c.reduceTasks[i].state = taskRunning
			c.reduceTasks[i].startTime = time.Now()
			return i, true
		}
	}
	return -1, false
}

func (c *Coordinator) canAssignTaskLocked(task *taskInfo) bool {
	if task.state == taskIdle {
		return true
	}
	if task.state == taskRunning && time.Since(task.startTime) > c.timeout {
		return true
	}
	return false
}

func (c *Coordinator) allMapDoneLocked() bool {
	for _, task := range c.mapTasks {
		if task.state != taskDone {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceDoneLocked() bool {
	for _, task := range c.reduceTasks {
		if task.state != taskDone {
			return false
		}
	}
	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allReduceDoneLocked()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]taskInfo, len(files)),
		reduceTasks: make([]taskInfo, nReduce),
		timeout:     10 * time.Second,
	}

	for i, fileName := range files {
		c.mapTasks[i] = taskInfo{
			fileName: fileName,
			state:    taskIdle,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = taskInfo{
			state: taskIdle,
		}
	}

	c.server(sockname)
	return &c
}
