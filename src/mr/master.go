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

type MasterState int

const (
	MAPMASTER MasterState = iota
	REDUCEMASTER
	DONEMASTER
)

type Master struct {
	state                MasterState
	unassignedMapTask    []Task
	unassignedReduceTask []Task
	assignedWorker       map[int]Task
	sync.Mutex
}

/*
func (m *Master) ReExecute() {
	for {
		time.Sleep(10 * time.Second)
		m.Lock()
		defer m.Unlock()
		if m.state == DONEMASTER {
			break
		}
		t := time.Now()
		for taskId, task := range m.assignedWorker {
			// TODO: make this a param
			if t.Sub(task.T) > (5 * time.Second) {
				log.Printf("Reassign task")
				m.unassignedTask = append(m.unassignedTask, task)
				delete(m.assignedWorker, taskId)
			}
		}
	}
}
*/

// TODO: add heartbeat rpc
// TODO: skip empty reduce task
func (m *Master) GetTask(args *TaskArgs, task *Task) error {
	m.Lock()
	defer m.Unlock()

	if args.Done {
		delete(m.assignedWorker, args.Id)
		for k, v := range args.Outputs {
			m.unassignedReduceTask[k].Inputs = append(m.unassignedReduceTask[k].Inputs, v)
		}
	}
	t := time.Now()

	switch m.state {
	case MAPMASTER:
		{
			if len(m.unassignedMapTask) == 0 {
				if len(m.assignedWorker) == 0 {
					m.state = REDUCEMASTER
				} else {
					for taskid, task := range m.assignedWorker {
						// todo: make this a param
						if t.Sub(task.T) > (5 * time.Second) {
							m.unassignedMapTask = append(m.unassignedMapTask, task)
							delete(m.assignedWorker, taskid)
						}
					}

				}
			}
			if len(m.unassignedMapTask) > 0 {
				*task = m.unassignedMapTask[len(m.unassignedMapTask)-1]
				task.T = t
				m.unassignedMapTask = m.unassignedMapTask[:len(m.unassignedMapTask)-1]
				m.assignedWorker[task.Id] = *task
				return nil
			} else {
				task.Type = IDLE
				return nil
			}

		}
		// TODO fall through
	case REDUCEMASTER:
		{
			if len(m.unassignedReduceTask) == 0 {
				if len(m.assignedWorker) == 0 {
					m.state = DONEMASTER
					task.Type = DONE
					return nil
				} else {
					for taskid, task := range m.assignedWorker {
						// todo: make this a param
						if t.Sub(task.T) > (5 * time.Second) {
							log.Printf("reassign reduce task")
							m.unassignedReduceTask = append(m.unassignedReduceTask, task)
							delete(m.assignedWorker, taskid)
						}
					}

				}
			}
			if len(m.unassignedReduceTask) > 0 {
				*task = m.unassignedReduceTask[len(m.unassignedReduceTask)-1]
				task.T = t
				m.unassignedReduceTask = m.unassignedReduceTask[:len(m.unassignedReduceTask)-1]
				m.assignedWorker[task.Id] = *task
				return nil
			} else {
				task.Type = IDLE
				return nil
			}

		}
	case DONEMASTER:
		{
			task.Type = DONE
			return nil
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.Lock()
	defer m.Unlock()
	return m.state == DONEMASTER
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// TODO: better divide number mapper
	m.state = MAPMASTER
	m.unassignedMapTask = make([]Task, 0)
	m.assignedWorker = make(map[int]Task)
	for i, file := range files {
		task := Task{}
		task.Id = i
		task.Type = MAP
		task.Inputs = append(task.Inputs, file)
		task.NReduce = nReduce
		m.unassignedMapTask = append(m.unassignedMapTask, task)
	}
	m.unassignedReduceTask = make([]Task, 0)
	// Generate reduce tasks
	for i := 0; i < nReduce; i++ {
		rTask := Task{}
		rTask.Type = REDUCE
		rTask.Id = i
		m.unassignedReduceTask = append(m.unassignedReduceTask, rTask)
	}

	// go m.ReExecute()
	m.server()
	return &m
}
