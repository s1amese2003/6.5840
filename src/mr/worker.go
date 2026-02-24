package mr

import "encoding/json"
import "fmt"
import "hash/fnv"
import "io"
import "log"
import "net/rpc"
import "os"
import "sort"
import "time"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		req := RequestTaskArgs{}
		reply := RequestTaskReply{}
		if !call("Coordinator.RequestTask", &req, &reply) {
			return
		}

		switch reply.TaskType {
		case TaskMap:
			if runMapTask(reply.TaskID, reply.FileName, reply.NReduce, mapf) {
				doneReq := ReportTaskArgs{TaskType: TaskMap, TaskID: reply.TaskID}
				doneReply := ReportTaskReply{}
				call("Coordinator.ReportTask", &doneReq, &doneReply)
			}
		case TaskReduce:
			if runReduceTask(reply.TaskID, reply.NMap, reducef) {
				doneReq := ReportTaskArgs{TaskType: TaskReduce, TaskID: reply.TaskID}
				doneReply := ReportTaskReply{}
				call("Coordinator.ReportTask", &doneReq, &doneReply)
			}
		case TaskWait:
			time.Sleep(500 * time.Millisecond)
		case TaskExit:
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func runMapTask(taskID int, fileName string, nReduce int, mapf func(string, string) []KeyValue) bool {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("worker: open map input %s failed: %v", fileName, err)
		return false
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		log.Printf("worker: read map input %s failed: %v", fileName, err)
		return false
	}

	kva := mapf(fileName, string(content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}

	for reduceID := 0; reduceID < nReduce; reduceID++ {
		tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-%d-%d-", taskID, reduceID))
		if err != nil {
			log.Printf("worker: create temp map output failed: %v", err)
			return false
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range buckets[reduceID] {
			if err := enc.Encode(&kv); err != nil {
				tempFile.Close()
				os.Remove(tempFile.Name())
				log.Printf("worker: write map output failed: %v", err)
				return false
			}
		}
		if err := tempFile.Close(); err != nil {
			os.Remove(tempFile.Name())
			log.Printf("worker: close temp map output failed: %v", err)
			return false
		}

		outName := fmt.Sprintf("mr-%d-%d", taskID, reduceID)
		if err := os.Rename(tempFile.Name(), outName); err != nil {
			os.Remove(tempFile.Name())
			log.Printf("worker: rename map output failed: %v", err)
			return false
		}
	}
	return true
}

func runReduceTask(taskID int, nMap int, reducef func(string, []string) string) bool {
	intermediate := []KeyValue{}
	for mapID := 0; mapID < nMap; mapID++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapID, taskID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("worker: open reduce input %s failed: %v", fileName, err)
			return false
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				file.Close()
				log.Printf("worker: decode reduce input %s failed: %v", fileName, err)
				return false
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-out-%d-", taskID))
	if err != nil {
		log.Printf("worker: create temp reduce output failed: %v", err)
		return false
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		if _, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output); err != nil {
			tempFile.Close()
			os.Remove(tempFile.Name())
			log.Printf("worker: write reduce output failed: %v", err)
			return false
		}

		i = j
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())
		log.Printf("worker: close temp reduce output failed: %v", err)
		return false
	}

	outName := fmt.Sprintf("mr-out-%d", taskID)
	if err := os.Rename(tempFile.Name(), outName); err != nil {
		os.Remove(tempFile.Name())
		log.Printf("worker: rename reduce output failed: %v", err)
		return false
	}
	return true
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Printf("dialing failed: %v", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call %s failed err %v", os.Getpid(), rpcname, err)
	return false
}
