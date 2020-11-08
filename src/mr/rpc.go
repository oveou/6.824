package mr

import (
	"os"
	"strconv"
	"time"
)

// enum task type
type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	IDLE
	DONE
)

type TaskArgs struct {
	Done    bool
	Id      int
	Outputs map[int]string
}

type Task struct {
	Type    TaskType
	Id      int
	Inputs  []string
	T       time.Time
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
