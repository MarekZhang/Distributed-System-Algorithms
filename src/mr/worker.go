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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

// for sorting by key
func (a ByKey) Len() int 		   { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	haswork := true
	//dead loop until all work is done
	for haswork{
		//	// Your worker implementation here.
		//	// return the filename and func name(Map or Reduce)
		args := Args{}
		reply := Reply{}
		// return false if cannot connect with master through rpc
		connected := call("Master.JobSchedule", &args, &reply)
		if !connected {
			fmt.Println("all work done, exit")
			os.Exit(0)
		}

		haswork = !reply.Allworkdone
		funcname := reply.Funcname

		if funcname == "map" {
			filename := reply.Task.File_list
			mapnumber := reply.Task.Map_no
			file, err := os.Open(filename[0])
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			fmt.Println("worker is working on mapping", filename) //filename == original file
			keyValuesArr := mapf(filename[0], string(content))

			// store intermediate key-value into N(number of reduce) files
			nreduce := reply.Nreduce
			ofiles := []*os.File{}
			reduceTasks := make([]string, nreduce)
			// create n reduce tasks

			for i := 0; i < nreduce; i++ {
				oname := "mr-" + strconv.Itoa(mapnumber) + "-" + strconv.Itoa(i)
				ofile, _ := os.Create(oname)
				ofiles = append(ofiles, ofile)
				defer ofile.Close()
				//add intermediate key-value files for assigning to reduce workder
				reduceTasks[i] = oname
			}

			// partition intermediate key-value into n(reduce no)
			encoders := make([]*json.Encoder, len(ofiles))
			for idx, ofile := range ofiles {
				encoders[idx] = json.NewEncoder(ofile)
			}

			// encoded intermediate key-value in json format
			for _, kv := range keyValuesArr {
				// partition current map output into N(number of reduce tasks) files
				idx := ihash(kv.Key) % nreduce
				err := encoders[idx].Encode(&kv)
				if err != nil {
					log.Fatal("Unable to encode % v", kv)
				}
			}

			args := Args{}
			args.Mapwork_no = reply.Task.Map_no
			args.ReduceTasks = reduceTasks
			call("Master.MapDone", &args, &reply) // add reduce task
			fmt.Println(reply.Task.Map_no ,"map completed")

		} else if funcname == "reduce" {
			// if map task has not been completed periodically block the reduce worker
			mapDone := false
			for !mapDone {
				time.Sleep(time.Second)
				call("Master.MapIsDone", &mapDone, &mapDone)
			}

			// traverse all intermediate key-value file
			reduceFiles := reply.Task.File_list
			nreduce := reply.Nreduce
			idx := reply.Task.Reduce_no
			fmt.Println("reduce start", idx)

			kva := []KeyValue{}
			for _, filename := range reduceFiles {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				// gather all key values for current reduce task
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil{
						break
					}
					if ihash(kv.Key) % nreduce == idx	{
						kva = append(kva, kv)
					}
				}
				// sort
				sort.Sort(ByKey(kva))
				// save result
				oname := "mr-out-" + strconv.Itoa(idx)
				ofile, _ := os.Create(oname)

				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++{
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}

				ofile.Close()

				args := Args{}
				args.Reducework_no = idx
				call("Master.ReduceDone", &args, &reply)
			}
		}
	}
}

// worker ask for a job(whether map or reduce depends on the return value)
func CallForJob(args *Args, reply *Reply) bool{
	return call("Master.JobSchedule", args, reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
