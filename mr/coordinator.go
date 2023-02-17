package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce      int             // Number of reduce tasks
	Files          []string        // Files for map tasks, len(Files) is number of Map tasks
	MapTasks       chan MapTask    // Channel for uncompleted map tasks
	CompletedTasks map[string]bool // Map to check if task is completed
	Lock           sync.Mutex      // Lock for contolling shared variables
	MapTasksCompleted bool         // Check if all map tasks are completed
	ReduceTasks	   chan ReduceTask // Channel for uncompleted reduce tasks
	CompletedReduceTasks map[string]bool // Map to check if reduce task is completed
	ReduceCompleted bool           // bool to check if all reduce tasks have been completed so that we can move on
}

func (c *Coordinator) CheckIfMapFinished(args *EmptyArs, reply *MapTasksFinishedReply) {
	reply.Finished = c.MapTasksCompleted
}

// Starting coordinator logic
func (c *Coordinator) Start() {
	fmt.Println("Starting Coordinator, adding Map Tasks to channel")

	for i, file := range c.Files {
		mapTask := MapTask{
			Filename:  file,
			NumReduce: c.NumReduce,
			MapTaskNumber: i,
		}
		c.MapTasks <- mapTask
		c.CompletedTasks["map_"+mapTask.Filename] = false
	}

	c.server()
}

func (c *Coordinator) StartReduceTasks() {
	fmt.Println("Starting Reduce Tasks, adding Reduce Tasks to channel")
	for i:= 0; i < c.NumReduce; i++ {
		reduceTask := ReduceTask {
			Partition: i,
			NumFiles: len(c.Files),
		}


		c.ReduceTasks <- reduceTask
		c.CompletedReduceTasks["mr-out-"+strconv.Itoa(i)] = false
	}
}

// RPC that worker calls when idle (worker requests a map task)
func (c *Coordinator) RequestMapTask(args *EmptyArs, reply *MapTask) error {
	task, _ := <-c.MapTasks // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

	*reply = task

	go c.WaitForWorker(task)

	return nil
}

// RPC that worker calls when idle (worker requests a reduce task)
func (c *Coordinator) RequestReduceTask(args *EmptyArs, reply *ReduceTask) error {

	task, _ := <-c.ReduceTasks
	*reply = task

	go c.WaitForReduceWorker(task)
	return nil
}

// Goroutine will wait 10 seconds and check if map task is completed or not
func (c *Coordinator) WaitForWorker(task MapTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["map_"+task.Filename] == false {
		fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}

func (c *Coordinator) WaitForReduceWorker(task ReduceTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedReduceTasks["mr-out-"+strconv.Itoa(task.Partition)] == false {
		fmt.Println("Timer expired, task", task.Partition, "is not finished. Putting back in queue.")
		c.ReduceTasks <- task
	}
	c.Lock.Unlock()
}

// RPC for reporting a completion of a task
func (c *Coordinator) TaskCompleted(args *MapTask, reply *MapTasksFinishedReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["map_"+args.Filename] = true

	mapTaskCompleted := true
	for task := range c.CompletedTasks {
		mapTaskCompleted = mapTaskCompleted && c.CompletedTasks[task]
	}

	c.MapTasksCompleted = mapTaskCompleted
	reply.Finished = c.MapTasksCompleted
	fmt.Println("c.MapTasksCompleted:", c.MapTasksCompleted)
	if mapTaskCompleted {
		c.StartReduceTasks()
	}

	return nil
}

func (c *Coordinator) ReduceTaskFinished(args *ReduceTask, reply *ReduceTaskFinishedReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.CompletedReduceTasks["mr-out-"+strconv.Itoa(args.Partition)] = true
	reduceTasksCompleted := true
	for task := range c.CompletedReduceTasks {
		reduceTasksCompleted = reduceTasksCompleted && c.CompletedReduceTasks[task]
	}

	if reduceTasksCompleted {
		c.ReduceCompleted = true
	}
	reply.Finished = reduceTasksCompleted

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
// Need a task for All Reduce Tasks to be done and return true
func (c *Coordinator) Done() bool {


	return c.ReduceCompleted
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:      nReduce,
		Files:          files,
		MapTasks:       make(chan MapTask, 100),
		CompletedTasks: make(map[string]bool),
		ReduceTasks: make(chan ReduceTask, 100),
		CompletedReduceTasks: make(map[string] bool),
		ReduceCompleted: false,
	}

	// fmt.Println("Starting coordinator")

	c.Start()

	return &c
}

