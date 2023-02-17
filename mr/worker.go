package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
	}
	
	w.RequestMapTask()
}

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) RequestMapTask() {
	for {
		args := EmptyArs{}
		reply := MapTask{}
		call("Coordinator.RequestMapTask", &args, &reply)

		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()


		kva := w.mapf(reply.Filename, string(content))

		
		// store kva in multiple files according to rules described in the README
		// ...
		buckets := make([][]KeyValue, reply.NumReduce)
		for _, keyVal := range kva {
			v := ihash(keyVal.Key) % reply.NumReduce
			buckets[v] = append(buckets[v], keyVal)
		}

		for i:= 0; i < reply.NumReduce; i++ {
			filename := "mr-" + strconv.Itoa(reply.MapTaskNumber) + "-" + strconv.Itoa(i % reply.NumReduce)
			file, _ := os.Create(filename)
			enc := json.NewEncoder(file)
			
			for _, KeyValue := range buckets[i] {
				err := enc.Encode(&KeyValue)
				if(err != nil) {
					fmt.Print("Error in encoding: ",err)
				}
			}
			file.Close()
		}


		// Send a message to coordinator that maps are finished for given filename
		MapTasksFinishedReply := MapTasksFinishedReply{}
		call("Coordinator.TaskCompleted", &reply, &MapTasksFinishedReply)

		if MapTasksFinishedReply.Finished {
			break
		}
		
	}

	w.RequestReduceTask()
}

// Requests reduce task, tries to do it, and repeats
func (w *WorkerSt) RequestReduceTask() {
	for {
		args := EmptyArs{}
		reply := ReduceTask{}
		call("Coordinator.RequestReduceTask", &args, &reply)
		kva := []KeyValue{}
		for x:= 0; x < reply.NumFiles; x ++ {
			filename := "mr-"+strconv.Itoa(x) + "-" + strconv.Itoa(reply.Partition)
			// fmt.Println(filename)
			file, _ := os.Open(filename)
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
				  break
				}
				kva = append(kva, kv)
			  } 
			file.Close()
		}
		sort.Sort(ByKey(kva))
		// POTENTIAL ERROR HERE APPARENTLY IN PIAZZA
		oname := "mr-out-" + strconv.Itoa(reply.Partition)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := w.reducef(kva[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			i = j
		}
		ofile.Close()

		// Send a message to coordinator that maps are finished for given filename
		ReduceTaskFinishedReply := ReduceTaskFinishedReply{}
		call("Coordinator.ReduceTaskFinished", &reply, &ReduceTaskFinishedReply)

		if ReduceTaskFinishedReply.Finished {
			break
		}
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
