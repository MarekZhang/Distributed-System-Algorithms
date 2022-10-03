package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Master struct {
	// Your definitions here.
	allWorkDone 	bool
	mapWorkDone 	bool
	allMapAssigned  bool
	reduceWorkDone  bool
	mapTaskList 	[]*Task
	reduceTaskList	[]*Task
	map_no			int // current map task number
	reduce_no		int // current reduce task number
	nreduce  		int
	mu 				sync.Mutex


	//mapWorkList map[string]int
	//mapCompleted []bool
	//reduceCompleted []bool
	//reduceWorkList map[int]bool
	//intermediateList []string
}

// map/reduce task type, including task status, file position, start time(checking if the task is overtime)
type Task struct {
	Status 			int
	Start_time    	time.Time
	File_list		[]string
	Map_no			int
	Reduce_no		int
}

// enumerate task status value
const (
	IDLE       = iota
	INPROGRESS = iota
	COMPLETED  = iota
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
var tempt int = 2
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = tempt
	time.Now()
	return nil
}

// Reduce worker communicate with master to check out if all map work is done
func (m *Master) MapIsDone (args *bool, reply *bool) error {
	Mas.mu.Lock()
	defer Mas.mu.Unlock()
	*reply = Mas.mapWorkDone
	return nil
}

// master schedule job for workers
func (m *Master) JobSchedule(args *Args, reply *Reply) error {
	// assessment if all work done reply.allWorkDone = true
	Mas.mu.Lock()
	defer Mas.mu.Unlock()
	reply.Allworkdone = Mas.allWorkDone

	// assign task for workers
	if Mas.allMapAssigned {
		// reduce task
		reply.Nreduce = Mas.nreduce
		for _, task := range Mas.reduceTaskList {
			if task.Status == IDLE {
				task.Status = INPROGRESS
				task.Start_time = time.Now()
				reply.Task = task
				reply.Funcname = "reduce"
				reply.Nreduce = Mas.nreduce
				return nil
			}
		}
	} else {
		// map task
		for _, task := range Mas.mapTaskList {
			if task.Status == IDLE {
				task.Status = INPROGRESS
				task.Start_time = time.Now()
				reply.Task = task
				reply.Funcname = "map"
				reply.Nreduce = Mas.nreduce
				return nil
			}
		}
		Mas.allMapAssigned = true
	}

	time.Sleep(time.Second * 2)

	return nil
}

func (m *Master) MapDone(args *Args, reply *Reply) error{
	Mas.mu.Lock()
	map_no := args.Mapwork_no
	Mas.mapTaskList[map_no].Status = COMPLETED
	reduceTaskList := Mas.reduceTaskList
	for idx, filename := range args.ReduceTasks {
		reduceTaskList[idx].File_list = append(reduceTaskList[idx].File_list, filename)
	}
	mapworkdone := true
	for _, task := range Mas.mapTaskList  {
		// check if all map task completed
		if task.Status != COMPLETED {
			mapworkdone = false
		}
	}
	Mas.mapWorkDone = mapworkdone
	Mas.mu.Unlock()

	return nil
}

func (m *Master) ReduceDone(args *Args, reply * Reply) error{
	Mas.mu.Lock()
	reduce_no := args.Reducework_no
	Mas.reduceTaskList[reduce_no].Status = COMPLETED
	reduceworkdone := true
	for _, task := range Mas.reduceTaskList {
		if task.Status != COMPLETED {
			reduceworkdone = false
		}
	}
	Mas.reduceWorkDone = reduceworkdone
	defer Mas.mu.Unlock()

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
	// main/mrmaster.go calls this function
	// Your code here.
	if Mas.reduceWorkDone {
		Mas.allWorkDone = true
	}

	// remove all intermediate key-value files
	if Mas.allWorkDone {
		files, err := ioutil.ReadDir("./")
		if err != nil {
			log.Fatal(err)
		}
		for _, file	:= range files {
			filename := file.Name()
			matched, err := regexp.MatchString("mr-[0-9]+-*", filename)
			if err != nil {
				fmt.Println("regex match error")
			}
			if matched {
				e := os.Remove(filename)
				if e != nil {
					fmt.Printf("remove file %v error", filename)
				}
			}
		}

		// all work done, exit
		os.Exit(0)
	}

	// periodically ping workers (reset work if timeout > 10s)
	for _, task := range Mas.mapTaskList {
		now := time.Now()
		if task.Status == INPROGRESS && now.Second() - task.Start_time.Second() > 10  {
			task.Status = IDLE
		}
	}

	for _, task := range Mas.reduceTaskList {
		now := time.Now()
		if task.Status == INPROGRESS && now.Second() - task.Start_time.Second() > 10 {
			task.Status = IDLE
		}
	}

	// equally assign map and reduce tasks in order to run them in parallel
	ret := Mas.allWorkDone

	return ret
}

//
// create a Master..
// nReduce is the number of reduce tasks to use.
//
var Mas *Master
func MakeMaster(files []string, nReduce int) *Master {
	// Master initialization
	Mas = new(Master)
	Mas.allWorkDone = false
	Mas.mapWorkDone = false

	Mas.mapTaskList = []*Task{}
	Mas.reduceTaskList = []*Task{}
	// initialize map task list
	for idx, filename := range files {
		position := "./" + filename
		task := &Task{
			IDLE,
			time.Now(),
			[]string{position},
			idx,
			-1,
		}
		Mas.mapTaskList = append(Mas.mapTaskList, task)
	}

	Mas.nreduce = nReduce

	for i := 0; i < nReduce; i++ {
		task := & Task{
			IDLE,
			time.Now(),
			[]string{},
			-1,
			i,
		}
		Mas.reduceTaskList = append(Mas.reduceTaskList, task)

	}

	Mas.server()
	return Mas
}