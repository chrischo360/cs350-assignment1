package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type EmptyArs struct {
}

type EmptyReply struct {
}

type MapTasksFinishedReply struct {
	Finished bool
}

type ReduceTaskFinishedReply struct {
	Finished bool
}

type ReduceTask struct {
	Partition int // The reduce file number
	NumFiles int // The intermediate file number aka MR-?-Y
	// Filename string
	// NumReduce int
	// ReduceTaskNumber int
}

// Universal Task structure
type MapTask struct {
	Filename  string // Filename = key
	NumReduce int    // Number of reduce tasks, used to figure out number of buckets
	MapTaskNumber int // Map Task number for assigning file
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
