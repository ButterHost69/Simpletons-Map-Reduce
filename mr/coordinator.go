// mr-test.sh:  Crash Test Passes Sometime
package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type OngoingTasks struct {
	Started time.Time
	Task    string
}
type Coordinator struct {
	// Your definitions here.
	NReduce            int
	PendingMapTasks    []string
	PendingReduceTasks []string
	PendingSortTasks   []string

	OnGoingSortTasks   []OngoingTasks
	OnGoingReduceTasks []OngoingTasks
	OnGoingMapTasks    []OngoingTasks
	DoneTasksCount     int
}

// Helper Functions
// const (
// 	IFLOG =	false
// )
// func logMessage(message string ...) {
//     if IFLOG {
//         log.Printf("%s\n", message)
//     }
// }

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Send Tasks to Workers
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// fmt.Println("[Coordinator] Get Task Request Recieved...")
	remainingMaps := len(c.PendingMapTasks)
	remainingReduces := len(c.PendingReduceTasks)
	remainingSorts := len(c.PendingSortTasks)

	if remainingMaps != 0 {
		task := strings.Split(c.PendingMapTasks[0], ":")
		reply.Operation = task[0]
		reply.Filepath = task[1]
		reply.NReduce = c.NReduce

		// c.OnGoingMapTasks = append(c.OnGoingMapTasks, c.PendingMapTasks[0])
		c.OnGoingMapTasks = append(c.OnGoingMapTasks, OngoingTasks{
			Started: time.Now(),
			Task:    c.PendingMapTasks[0],
		})
		c.PendingMapTasks = c.PendingMapTasks[1:]

		fmt.Printf("[Coordinator] Task Provided: %s:%s\n", reply.Operation, reply.Filepath)
		fmt.Println("[Coordinator] Pending MAP Tasks: \n", c.PendingMapTasks)
	} else if remainingSorts != 0 && len(c.OnGoingMapTasks) == 0 {

		task := strings.Split(c.PendingSortTasks[0], ":")
		reply.Operation = task[0]
		reply.Filepath = task[1]
		reply.NReduce = c.NReduce

		// c.OnGoingSortTasks = append(c.OnGoingSortTasks, c.PendingSortTasks[0])
		c.OnGoingSortTasks = append(c.OnGoingSortTasks, OngoingTasks{
			Started: time.Now(),
			Task:    c.PendingSortTasks[0],
		})
		c.PendingSortTasks = c.PendingSortTasks[1:]

		fmt.Printf("[Coordinator] Task Provided: %s:%s\n", reply.Operation, reply.Filepath)
		fmt.Println("[Coordinator] Pending SORT Tasks: \n", c.PendingReduceTasks)
	} else if remainingReduces != 0 && len(c.OnGoingSortTasks) == 0 {

		task := strings.Split(c.PendingReduceTasks[0], ":")
		reply.Operation = task[0]
		reply.Filepath = task[1]
		reply.NReduce = c.NReduce

		// c.OnGoingReduceTasks = append(c.OnGoingReduceTasks, c.PendingReduceTasks[0])
		c.OnGoingReduceTasks = append(c.OnGoingReduceTasks, OngoingTasks{
			Started: time.Now(),
			Task:    c.PendingReduceTasks[0],
		})
		c.PendingReduceTasks = c.PendingReduceTasks[1:]

		fmt.Printf("[Coordinator] Task Provided: %s:%s\n", reply.Operation, reply.Filepath)
		fmt.Println("[Coordinator] Pending REDUCE Tasks: \n", c.PendingReduceTasks)
	} else {
		fmt.Println("[Coordinator] No Tasks Available")
		reply.Operation = ""
		reply.Filepath = ""
	}
	return nil
}

// Revcieve Notification
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	// fmt.Println("[Coordinator] Recieved Task Done Notification From Worker")
	switch strings.Split(args.Task, ":")[0] {
	case "map":
		{
			for idx, task := range c.OnGoingMapTasks {
				if task.Task == args.Task {
					c.OnGoingMapTasks = append(c.OnGoingMapTasks[:idx], c.OnGoingMapTasks[idx+1:]...)
					fmt.Printf("[Coordinator] Task Done Type - %s\n", args.Task)
					for _, fp := range args.FilePaths {
						ifQueued := false
						redTask := fmt.Sprintf("sort:%s", fp)
						for _, task := range c.PendingSortTasks {
							if task == redTask {
								ifQueued = true
							}
						}
						if !ifQueued {
							c.PendingSortTasks = append(c.PendingSortTasks, fmt.Sprintf("sort:%s", fp))
						}
					}
				}
			}
		}
	case "sort":
		{
			fmt.Printf("[Coordinator] Task Done Type - %s\n", args.Task)
			for idx, task := range c.OnGoingSortTasks {
				if task.Task == args.Task {
					c.OnGoingSortTasks = append(c.OnGoingSortTasks[:idx], c.OnGoingSortTasks[idx+1:]...)
					c.PendingReduceTasks = append(c.PendingReduceTasks, fmt.Sprintf("reduce:%s", strings.Split(args.Task, ":")[1]))
				}
			}
		}
	case "reduce":
		{
			fmt.Printf("[Coordinator] Task Done Type - %s\n", args)
			for idx, task := range c.OnGoingReduceTasks {
				if task.Task == args.Task {
					c.OnGoingReduceTasks = append(c.OnGoingReduceTasks[:idx], c.OnGoingReduceTasks[idx+1:]...)
				}
			}
			fmt.Println("[Coordinator] On Going Reduce Tasks - ", args)
			c.DoneTasksCount += 1
		}

	}
	reply.Ack = true
	return nil
}

func (c *Coordinator) GetFileContent(args *GetFileContentArgs, reply *GetFileContentReply) error {
	fmt.Println("[Coordinator] Get File Content Request Recieved...")
	file, err := os.Open(args.Filepath)
	if err != nil {
		fmt.Printf("[Coordinator] Error in reading File - %s\nError: %v", args.Filepath, err)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("[Coordinator] cannot read: ", args.Filepath)
		return err
	}
	file.Close()

	reply.Content = content
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	fmt.Println("Coordinator Started ...")
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	// Check and realot task
	for idx, task := range c.OnGoingMapTasks {
		timeTaken := time.Since(task.Started)
		if timeTaken >= (time.Second * 10) {
			mapTasks := len(c.OnGoingMapTasks)
			if mapTasks == 1 {
				c.OnGoingMapTasks = make([]OngoingTasks, 0)
			} else if mapTasks == 2 {
				if idx == 0 { // If Removing First Element
					c.OnGoingMapTasks = []OngoingTasks{c.OnGoingMapTasks[1]}
				} else { // If Removing Last Element
					c.OnGoingMapTasks = []OngoingTasks{c.OnGoingMapTasks[0]}
				}
			} else {
				c.OnGoingMapTasks = append(c.OnGoingMapTasks[:idx], c.OnGoingMapTasks[idx+1:]...)
			}
			c.PendingMapTasks = append(c.PendingMapTasks, task.Task)
			fmt.Println("Updated PENDING Reduce: ", c.PendingReduceTasks)
		}
	}

	for idx, task := range c.OnGoingSortTasks {
		timeTaken := time.Since(task.Started)
		if timeTaken >= (time.Second * 10) {
			sortTasks := len(c.OnGoingSortTasks)
			if sortTasks == 1 {
				c.OnGoingSortTasks = make([]OngoingTasks, 0)
			} else if sortTasks == 2 {
				if idx == 0 { // If Removing First Element
					c.OnGoingSortTasks = []OngoingTasks{c.OnGoingSortTasks[1]}
				} else { // If Removing Last Element
					c.OnGoingSortTasks = []OngoingTasks{c.OnGoingSortTasks[0]}
				}
			} else {
				c.OnGoingSortTasks = append(c.OnGoingSortTasks[:idx], c.OnGoingSortTasks[idx+1:]...)
			}
			c.PendingSortTasks = append(c.PendingSortTasks, task.Task)
			fmt.Println("Updated PENDING sort: ", c.PendingReduceTasks)
		}
	}

	for idx, task := range c.OnGoingReduceTasks {
		timeTaken := time.Since(task.Started)
		if timeTaken >= (time.Second * 10) {
			reduceTasks := len(c.OnGoingReduceTasks)
			if reduceTasks == 1 {
				c.OnGoingReduceTasks = make([]OngoingTasks, 0)
			} else if reduceTasks == 2 {
				if idx == 0 { // If Removing First Element
					c.OnGoingReduceTasks = []OngoingTasks{c.OnGoingReduceTasks[1]}
				} else { // If Removing Last Element
					c.OnGoingReduceTasks = []OngoingTasks{c.OnGoingReduceTasks[0]}
				}
			} else {
				c.OnGoingReduceTasks = append(c.OnGoingReduceTasks[:idx], c.OnGoingReduceTasks[idx+1:]...)
			}
			c.PendingReduceTasks = append(c.PendingReduceTasks, task.Task)
			fmt.Println("Updated PENDING Reduce: ", c.PendingReduceTasks)
		}
	}

	if c.DoneTasksCount == c.NReduce {
		ret = true
		fmt.Println("[Coordinator] MAP REDUCE TASK COMPLETED")
		fmt.Println("[Coordinator] Shutting Down")
	}

	return ret
}

// Helper Methods
func WriteToFile(fp string, content []byte) {
	newFile, err := os.Create(fp)
	if err != nil {
		fmt.Println("Could Not Create File ", fp, "...\nErr: ", err)
	}
	newFile.Write(content)
	newFile.Close()

}

const (
	MB = int(1e6)
)

func PartitionData(content []byte, count int, fileList []string) []string {
	if len(content) > 64*MB {
		newFileList := PartitionData(content[64*MB:], count+1, fileList)

		fp := fmt.Sprintf("mr-p-%d", count)
		WriteToFile(fp, content[:64*MB])
		return append(newFileList, fp)
	} else {
		fp := fmt.Sprintf("mr-p-%d", count)
		WriteToFile(fp, content)
		return append(fileList, fp)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	fmt.Println("[Coordinator] NReduce : ", nReduce)
	c.NReduce = nReduce

	// Your code here.

	//TODO: See if File Reading and Spliting and Storing Streamable
	// ------------------------------------------------------------------------
	// Partioning According to paper - not needed here
	// var content []byte
	// for _, filename := range os.Args[2:] {
	// 	file, err := os.Open(filename)
	// 	file.Seek(0, 0)
	// 	if err != nil {
	// 		log.Fatalf("cannot open %v", filename)
	// 	}
	// 	data, err := ioutil.ReadAll(file)
	// 	if err != nil {
	// 		log.Fatalf("cannot read %v", filename)
	// 	}
	// 	file.Close()
	//
	// 	// If content size > 64MB Partion content
	// 	content = append(content, data...)
	// }
	// count := 1
	// empty := make([]string, 0)
	// filesList := PartitionData(content, count, empty)
	// ------------------------------------------------------------------------

	filesList := files
	fmt.Println("Task List = ", filesList)
	fmt.Println("Data Read and Partioned...")
	fmt.Println("Now Ready for Map Task...")
	for _, file := range filesList {
		c.PendingMapTasks = append(c.PendingMapTasks, fmt.Sprintf("map:%s", file))
	}
	fmt.Println("[Coordinatror] Pending Task List: ", c.PendingMapTasks)

	// os.Exit(-1)
	c.server()
	return &c
}
