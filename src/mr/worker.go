package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[i], a[j] }
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
		call("Master.JobSchedule", &args, &reply)
		haswork = !reply.allworkdone
		fmt.Println(reply.Funcname)
		funcname := reply.Funcname

		if funcname == "map" {
			filename := reply.Mapname
			mapnumber := reply.Mapnumber
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			fmt.Println("worker is working on mapping", filename) //filename == original file
			keyValuesArr := mapf(filename, string(content))

			// store intermediate key-value
			sort.Sort(ByKey(keyValuesArr))
			oname := "map-inter-" + strconv.Itoa(mapnumber)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(keyValuesArr) {
				fmt.Fprintf(ofile, "%v %v\n", keyValuesArr[i].Key, keyValuesArr[i].Value)
				i++
			}

			ofile.Close()

			args := Args{}
			args.Mapname = filename
			call("Master.MapDone", &args, &reply)
			fmt.Println("map completed", oname)

		} else if funcname == "reduce" {
			fmt.Println("reduce start to work")
		}
	}
	// uncomment to send the Example RPC to the master.
	//CallExample()
}

// worker ask for a job(whether map or reduce depends on the return value)
func CallForJob(args *Args, reply *Reply) {
	call("Master.JobSchedule", args, reply)
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
