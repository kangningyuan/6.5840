package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

// 请求任务
// 执行map或reduce任务
// 处理文件输入输出
// 汇报任务状态

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	workerID := os.Getpid()

	for {
		// 请求任务
		task := getTask(workerID)

		switch task.TaskType {
		case MapTask:
			// 执行map任务
			doMap(task, mapf, workerID)
		case ReduceTask:
			// 执行reduce任务
			doReduce(task, reducef, workerID)
		case ExitTask:
			// 退出worker
			return
		case WaitTask:
			time.Sleep(500 * time.Millisecond)
			// 等待所有任务完成
			continue
		}
	}

}

func doMap(task GetTaskReply, mapf func(string, string) []KeyValue, workerID int) {
	// 读取文件内容
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("doMap: cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("doMap: cannot read %v", filename)
	}
	defer file.Close()

	// 调用map函数处理文件内容
	kva := mapf(filename, string(content))

	// 对map输出进行分区
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % task.NReduce
		intermediate[reduceTaskNum] = append(intermediate[reduceTaskNum], kv)
	}

	// 写入中间文件
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("doMap: cannot create %v", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("doMap: cannot encode %v", kv)
			}
		}
		file.Close()
	}
	// 汇报map任务完成
	reportTaskDone(task.TaskType, task.TaskID, workerID)
}

func doReduce(task GetTaskReply, reducef func(string, []string) string, workerID int) {
	reduceTaskNum := task.ReduceTaskNum
	mapTaskNum := task.MapTaskNum

	// 读取所有中间文件
	intermediate := make([]KeyValue, 0)
	for i := 0; i < mapTaskNum; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("doReduce: cannot open %v", filename)
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

	// 对中间键值对进行排序
	sort.Sort(ByKey(intermediate))

	// 创建输出文件
	filename := fmt.Sprintf("mr-out-%d", reduceTaskNum)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("doReduce: cannot create %v", filename)
	}

	// 对每一个key调用reduce函数
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 写入输出文件
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	defer file.Close()

	// 汇报reduce任务完成
	reportTaskDone(task.TaskType, task.TaskID, workerID)

}

func getTask(workerID int) GetTaskReply {
	args := GetTaskArgs{WorkerID: workerID}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

func reportTaskDone(taskType string, taskID int, workerID int) {
	args := ReportTaskArgs{
		TaskType:  taskType,
		WorkerID:  workerID,
		TaskID:    taskID,
		Completed: true,
	}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
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
