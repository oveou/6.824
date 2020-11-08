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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ImValue struct {
	tmpFile *os.File
	encoder *json.Encoder
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	task_done := false
	id := -1
	outPuts := make(map[int]string)
	for {
		args := TaskArgs{}
		args.Done = task_done
		args.Id = id
		args.Outputs = outPuts
		outPuts = make(map[int]string)
		id = -1
		task_done = false
		reply := Task{}
		if call("Master.GetTask", &args, &reply) {
			//			log.Printf("TaskType: %v, MapId: %v, Reduce: %v", reply.Type, reply.Id, reply.NReduce)
			switch reply.Type {
			case DONE:
				{
					log.Printf("Done")
					break
				}
			case IDLE:
				{
					log.Printf("Idle")
					time.Sleep(time.Second)
					continue
				}
			case MAP:
				{
					log.Printf("Working on Map %v", reply.Id)
					// TODO: buffer the input in memory
					intermediate := make(map[int]ImValue)

					for _, input := range reply.Inputs {
						file, err := os.Open(input)
						if err != nil {
							log.Fatalf("cannot open Map input file %v: %v", input, err)
						}
						content, err := ioutil.ReadAll(file)
						if err != nil {
							log.Fatalf("cannot read Map input file %v: %v", input, err)
						}
						file.Close()
						kva := mapf(input, string(content))
						for _, kv := range kva {
							i := ihash(kv.Key) % reply.NReduce
							if im, found := intermediate[i]; found {
								im.encoder.Encode(&kv)
							} else {
								//log.Printf("create a new temp file for %v:", fmt.Sprint("mr-", reply.Id, "-", i))
								im := ImValue{}
								im.tmpFile, err = ioutil.TempFile(".", fmt.Sprint("mr-", reply.Id, "-", i))
								if err != nil {
									log.Fatalf("cannot open temp file %v: %v", fmt.Sprint("mr-", reply.Id, "-", i), err)
								}
								im.encoder = json.NewEncoder(im.tmpFile)
								intermediate[i] = im
								im.encoder.Encode(&kv)
							}
						}
					}
					for k, im := range intermediate {
						im.tmpFile.Close()
						file := fmt.Sprint("mr-", reply.Id, "-", k)
						outPuts[k] = file
						err := os.Rename(im.tmpFile.Name(), file)
						if err != nil {
							log.Fatalf("cannot rename file %v", im.tmpFile.Name())
						}
					}
					task_done = true
					id = reply.Id
				}
			case REDUCE:
				{
					log.Printf("Working on Reduce %v", reply.Id)
					value := make(map[string][]string)
					for _, input := range reply.Inputs {
						file, err := os.Open(input)
						if os.IsNotExist(err) {
							log.Printf("reduce input not exist")
							continue
						}
						if err != nil {
							log.Fatalf("cannot open Reduce input %v: %v", input, err)
						}
						dec := json.NewDecoder(file)
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								break
							}
							value[kv.Key] = append(value[kv.Key], kv.Value)
						}
						file.Close()
					}
					keys := make([]string, 0)
					for k, _ := range value {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					oname := fmt.Sprint("mr-out-", reply.Id)
					ofile, _ := os.Create(oname)

					for _, k := range keys {
						output := reducef(k, value[k])
						fmt.Fprintf(ofile, "%v %v\n", k, output)
					}
					ofile.Close()
					task_done = true
					id = reply.Id
				}
			}
		}
	}
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
