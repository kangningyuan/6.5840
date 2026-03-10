package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 管理任务状态
// 处理worker的RPC请求
// 检测任务是否超时

type Task struct {
	FileName  string
	Status    string
	StartTime time.Time
	TaskID    int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	mapFinished bool
	allFinished bool
	files       []string
	nextTaskID  int
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查超时任务
	c.checkTimeout()

	// 如果任务都完成，通知worker退出
	if c.allFinished {
		reply.TaskType = ExitTask
		return nil
	}

	// 如果map任务没有执行完，分配map任务给worker
	if !c.mapFinished {
		for i, task := range c.mapTasks {
			if task.Status == Idle {
				reply.TaskType = MapTask
				reply.TaskID = task.TaskID
				reply.FileName = task.FileName
				reply.NReduce = c.nReduce

				//更新任务状态
				c.mapTasks[i].Status = TaskRunning
				c.mapTasks[i].StartTime = time.Now()
				return nil
			}
		}
		// map任务已全部分配但没有全部完成
		reply.TaskType = WaitTask
		return nil
	}

	// 如果map任务执行完毕，则分配reduce任务
	for i, task := range c.reduceTasks {
		if task.Status == Idle {
			reply.TaskType = ReduceTask
			reply.TaskID = task.TaskID
			reply.ReduceTaskNum = i
			reply.MapTaskNum = len(c.mapTasks)

			//更新任务状态
			c.reduceTasks[i].Status = TaskRunning
			c.reduceTasks[i].StartTime = time.Now()
			return nil
		}
	}
	// reduce任务已全部分配但没有全部完成
	reply.TaskType = WaitTask
	return nil
}

// 超时检查
func (c *Coordinator) checkTimeout() {
	timeout := time.Second * 10

	// 检查map任务超时
	if !c.mapFinished {
		isAllCompleted := true
		for i, task := range c.mapTasks {
			if task.Status == TaskRunning && time.Since(task.StartTime) > timeout {
				// 任务已超时，重置任务为待分配状态
				log.Printf("Map task %d timeout, reassigning...", task.TaskID)
				c.mapTasks[i].Status = Idle
			}
			if task.Status != TaskCompleted {
				isAllCompleted = false
			}
		}
		c.mapFinished = isAllCompleted
	}

	// 检查reduce任务超时
	isAllCompleted := true
	for i, task := range c.reduceTasks {
		if task.Status == TaskRunning && time.Since(task.StartTime) > timeout {
			// 任务已超时，重置任务为待分配状态
			log.Printf("Reduce task %d timeout, reassigning...", task.TaskID)
			c.reduceTasks[i].Status = Idle
		}
		if task.Status != TaskCompleted {
			isAllCompleted = false
		}
	}
	c.allFinished = isAllCompleted
}

// 报告任务状态
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新任务状态
	if args.TaskType == MapTask {
		for i, task := range c.mapTasks {
			if task.TaskID == args.TaskID && task.Status == TaskRunning {
				// 更新任务状态为已完成
				c.mapTasks[i].Status = TaskCompleted

				// 检查是否所有map任务都已完成
				isAllCompleted := true
				for _, task := range c.mapTasks {
					if task.Status != TaskCompleted {
						isAllCompleted = false
						break
					}
				}
				c.mapFinished = isAllCompleted
				reply.OK = true
				return nil
			}
		}
	} else if args.TaskType == ReduceTask {
		for i, task := range c.reduceTasks {
			if task.TaskID == args.TaskID && task.Status == TaskRunning {
				// 更新任务状态为已完成
				c.reduceTasks[i].Status = TaskCompleted

				// 检查是否所有reduce任务都已完成
				isAllCompleted := true
				for _, task := range c.reduceTasks {
					if task.Status != TaskCompleted {
						isAllCompleted = false
						break
					}
				}
				c.allFinished = isAllCompleted
				reply.OK = true
				return nil
			}
		}
	}
	// 未知任务类型
	reply.OK = false
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	if c.allFinished {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		mapFinished: false,
		allFinished: false,
		nextTaskID:  0,
	}
	// 初始化map任务
	for i, file := range c.files {
		c.mapTasks[i] = Task{
			FileName: file,
			Status:   Idle,
			TaskID:   i,
		}
	}
	// 初始化reduce任务
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Status: Idle,
			TaskID: i,
		}
	}

	c.server()
	return &c
}
