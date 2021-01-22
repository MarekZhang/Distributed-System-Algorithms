package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Master struct {
	// Your definitions here.
	allWorkDone bool
	mapWorkDone bool
	reduceWorkDone bool
	mapWorkList map[string]int
	mapCompleted []bool
	reduceCompleted []bool
	reduceWorkList map[int]bool
	intermediateList []string
	nreduce int
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
var tempt int = 2
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = tempt

	return nil
}

// master schedule job for workers
func (m *Master) JobSchedule(args *Args, reply *Reply) error {
	// assessment if all work done reply.allWorkDone = true
	if Mas.reduceWorkDone {
		Mas.allWorkDone = true
	}

	reply.allworkdone = Mas.allWorkDone
	if Mas.allWorkDone {
		os.Exit(0)
	}

	Mas.mu.Lock()
	defer Mas.mu.Unlock()
	if !Mas.mapWorkDone {
		for key, value := range Mas.mapWorkList {
			// assign a map task to the worker
			if !Mas.mapCompleted[value] {
				reply.Mapname = key
				reply.Mapnumber = value
				reply.Funcname = "map"
				return nil
			}
		}
		Mas.mapWorkDone = true
	} else {
		// assign a reduce task to the worker
		for key, value := range Mas.reduceWorkList {
			if !value {
				reply.NReduce = Mas.nreduce
				reply.Reducenumber = key
				reply.Funcname = "reduce"
				return nil
			}
		}
		Mas.reduceWorkDone = true
	}
	time.Sleep(time.Second * 2)

	return nil
}

func (m *Master) MapDone(args *Args, reply * Reply) error{
	Mas.mu.Lock()
	no := Mas.mapWorkList[args.Mapname]
	Mas.mapCompleted[no] = true
	Mas.mu.Unlock()

	return nil
}

func (m *Master) ReduceDone(args *Args, reply * Reply) error{
	Mas.mu.Lock()
	defer Mas.mu.Unlock()
	Mas.reduceWorkList[args.RedueceNo] = true

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
	Mas.mapWorkList = make(map[string]int)
	Mas.reduceWorkList = make(map[int]bool)
	Mas.nreduce = nReduce
	cnt := 0
	for _, filename := range files{
		Mas.mapWorkList[filename] = cnt
		cnt++
	}
	Mas.mapCompleted = make([]bool, cnt + 1)

	for N := 0; N < nReduce; N++ {
		Mas.reduceWorkList[N] = false
	}

	Mas.server()
	fmt.Println(Mas.mapWorkList)
	return Mas
}