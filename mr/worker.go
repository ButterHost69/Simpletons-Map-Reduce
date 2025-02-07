package mr

import (
	// "bytes"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Methods for sort function
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// type PartionKeyValue struct {
// 	PartNo	int
// 	KV 		KeyValue
// }

// type IntermediateKeyValue struct {
// 	TaskNo	int
// 	Kv		KeyValue
// }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	fmt.Println("Worker Started...")
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
get_task:
	fmt.Println("[Worker] Requesting Task...")
	ifAvailable, reply := CallGetTask()
	for !ifAvailable {
		// fmt.Println("[Worker] No Task ...")
		time.Sleep(3 * time.Second)
		fmt.Println("[Worker] Requesting Task...")
		ifAvailable, reply = CallGetTask()
	}

	fmt.Printf("[Worker] Task Recieved - %s:%s \n", reply.Operation, reply.Filepath)
	if reply.Operation == "map" {
		// intermediate := []KeyValue{}
		fmt.Println("[Worker] Retrieving File", reply.Filepath)
		file_content := CallGetFileContent(reply.Filepath)
		fmt.Println("[Worker] File-Content Size: ", len(file_content))
		fmt.Println("[Worker] Performing Map on the Provided File")
		intermediate := mapf(reply.Filepath, string(file_content))
		// TODO: DO we have to Partition From here (ihash) or somewhere else
		fmt.Println("[Worker] Intermediate: ", intermediate[:1])

		// Partition
		sort.Sort(ByKey(intermediate))
		partioned := make([][]KeyValue, reply.NReduce)
		for i := 0; i < reply.NReduce; i++ {
			partioned[i] = make([]KeyValue, 0)
		}

		for _, key := range intermediate {
			taskNo := ihash(key.Key) % reply.NReduce
			// fmt.Printf("[Worker] Map Partition TaskNo: %d (%s, %d)\n", taskNo, key.Key, ihash(key.Key))
			partioned[taskNo] = append(partioned[taskNo], key)
		}

		// fmt.Printf("[Worker] Partioned Data: \n\t%v\n\t%v\n\t%v\n", partioned[0][:3], partioned[1][:3], partioned[2][:3])

		fmt.Printf("[Worker] Partioned Data\n")
		intermediateFP := make([]string, 0)
		for i := 0; i < reply.NReduce; i++ {
			fName := fmt.Sprintf("mr-i-%d", i)
			// newFile, _ := os.Open(fName)
			newFile, err := os.OpenFile(fName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			// -----------------------
			enc := json.NewEncoder(newFile)
			if err != nil {
				fmt.Println("Could Not Create File...\nErr: ", err)
			}
			for _, kv := range partioned[i] {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Printf("[Worker] Cannot Store KV Pair in JSON, File: %s...Error:%v\n", reply.Filepath, err)
					break
				}
			}
			//  -----------------------

			//  -------------------------------------------------------------
			// jsonData, err := json.MarshalIndent(partioned[i], "", "  ")
			// if err != nil {
			// 	fmt.Println("Error encoding JSON:", err)
			// 	return
			// }

			// _, err = newFile.Write(jsonData)
			// if err != nil {
			// 	fmt.Println("Error writing to file:", err)
			// 	return
			// }
			//  -------------------------------------------------------------

			newFile.Close()
			intermediateFP = append(intermediateFP, fName)
		}

		fmt.Println("[Worker] Intermediate Pairs Written to File: ", intermediateFP)
		CallTaskDone(&TaskDoneArgs{
			Task:      fmt.Sprintf("%s:%s", reply.Operation, reply.Filepath),
			FilePaths: intermediateFP,
		})

	} else if reply.Operation == "sort" {
		fmt.Println("[Worker] Retrieving File", reply.Filepath)
		file_content := CallGetFileContent(reply.Filepath)
		fmt.Println("[Worker] File-Content Size: ", len(file_content))
		fmt.Println("[Worker] Performing SORT on the Provided File")

		// outFileName := fmt.Sprintf("mr-out-%s", string(reply.Filepath[len(reply.Filepath)-1]))
		outFP, err := os.Open(reply.Filepath)
		if err != nil {
			fmt.Printf("[Worker] Could Not Create file: %s\nError:%v\n", reply.Filepath, err)
		}

		kvs := make([]KeyValue, 0)

		reader := bytes.NewReader(file_content)
		dec := json.NewDecoder(reader)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}

		sort.Sort(ByKey(kvs))

		outFP.Seek(0, 0)
		enc := json.NewEncoder(outFP)
		if err != nil {
			fmt.Println("Could Not Create File...\nErr: ", err)
		}

		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("[Worker] Cannot Store KV Pair in JSON, File: %s...Error:%v\n", reply.Filepath, err)
				break
			}
		}

		outFP.Close()
		fmt.Printf("[Worker] Sort Task : %s Completed\n", reply.Filepath)
		CallTaskDone(&TaskDoneArgs{
			Task:      fmt.Sprintf("%s:%s", reply.Operation, reply.Filepath),
			FilePaths: []string{reply.Filepath},
		})

	} else if reply.Operation == "reduce" {
		fmt.Println("[Worker] Retrieving File", reply.Filepath)
		file_content := CallGetFileContent(reply.Filepath)
		fmt.Println("[Worker] File-Content Size: ", len(file_content))
		fmt.Println("[Worker] Performing Reduce on the Provided File - ", reply.Filepath)

		outFileName := fmt.Sprintf("mr-out-%s", string(reply.Filepath[len(reply.Filepath)-1]))
		outFP, err := os.Create(outFileName)
		if err != nil {
			fmt.Printf("[Worker] Could Not Create file: %s\nError:%v\n", outFileName, err)
		}

		kvs := make([]KeyValue, 0)
		// --------------------------------------------------
		// err = json.Unmarshal([]byte(file_content), &kvs)
		// if err != nil {
		// 	fmt.Println("Error decoding JSON:", err)
		// 	// return
		// }
		// --------------------------------------------------

		reader := bytes.NewReader(file_content)
		dec := json.NewDecoder(reader)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}

		sort.Sort(ByKey(kvs))

		fmt.Printf("[Worker] File: %s - no. kvs: %d\n", reply.Filepath, len(kvs))
		i := 0
		for i < len(kvs) {
			// pair := strings.Split(kvs[j], "   ")
			j := i + 1
			for j < len(kvs) && kvs[j].Key == kvs[i].Key {
				//fmt.Println("[Worker] Grouping Keys...")
				// for j < len(kvs) && strings.Split(kvs[j], "   ")[0] == strings.Split(kvs[i], "   ")[0] {
				j++
			}

			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kvs[k].Value)
			}
			//fmt.Println("[Worker] Performing Reduce For Key - ", strings.Split(kvs[i], "   ")[0])
			output := reducef(kvs[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(outFP, "%v %v\n", kvs[i].Key, output)

			i = j
		}

		outFP.Close()
		fmt.Printf("[Worker] Reduce Task : %s Completed\n", reply.Filepath)
		CallTaskDone(&TaskDoneArgs{
			Task:      fmt.Sprintf("%s:%s", reply.Operation, reply.Filepath),
			FilePaths: []string{outFileName},
		})

	}
	goto get_task
}
func CallTaskDone(args *TaskDoneArgs) {
	// fmt.Println("[Worker] Sending Task Done Notification")
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", args, &reply)
	if ok {
		fmt.Printf("[Worker] Notified Task Done Successful\n")
	} else {
		fmt.Printf("[Worker] Notify Task Done call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	fmt.Println("	-> Sending Example Request ...")
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetTask() (bool, TaskReply) {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		if reply.Operation != "" {
			return true, reply
		} else {
			return false, reply
		}
	} else {
		// Close the Worker Here in the Future
		fmt.Println("[Worker] Call Failed, Shutting Down Worker...")
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}

	return false, reply
}

func CallGetFileContent(fp string) []byte {
	args := GetFileContentArgs{
		Filepath: fp,
	}
	reply := GetFileContentReply{}

	ok := call("Coordinator.GetFileContent", &args, &reply)
	if ok {
		return reply.Content
	} else {
		// Close the Worker Here in the Future
		fmt.Printf("call failed!\n")
	}

	return reply.Content
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
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
