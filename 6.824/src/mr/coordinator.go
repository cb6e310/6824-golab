package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	file_list     []string
	mapped_file   map[string]bool
	reduced_index map[int]bool
	current_task  int
	nReduce       int
	isTimed       bool
	start_time    time.Time

	mu_map    sync.Mutex
	mu_reduce sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *MrArgs, reply *MrReply) error {
	if !c.isTimed {
		c.isTimed = true
		c.start_time = time.Now()
	}

	// fmt.Printf("in GetTask...\nfile number: %v\n", len(c.file_list))
	reply.Data = make([]string, 4)
	// fmt.Printf("len: %v\n", len(reply.Data))
	//	check the phase of task (map or reduce)
	c.mu_map.Lock()
	if len(c.mapped_file) == len(c.file_list) {
		c.current_task = 1
	} else {
		c.current_task = 0
	}

	// c.current_task = 1

	// fmt.Printf("current task: %v\n", c.current_task)
	if c.current_task == 0 {
		for file_index, file_name := range c.file_list {
			if !c.mapped_file[file_name] {
				c.mapped_file[file_name] = true
				c.mu_map.Unlock()
				// file_name = strings.Split(file_name, ".")[0]
				reply.Data[0] = file_name
				reply.Data[1] = strconv.Itoa(file_index)
				// reply.X = 5
				reply.Data[2] = "0"
				reply.Data[3] = strconv.Itoa(c.nReduce)
				// fmt.Printf("time to reply...\nfile name: %v\nmapped file: %v\n", reply.Data[0], c.mapped_file)
				return nil
			}
		}
	}
	c.mu_map.Unlock()

	c.mu_reduce.Lock()
	i := 0
	if c.current_task == 1 {
		for i < c.nReduce {
			if !c.reduced_index[i] {
				c.reduced_index[i] = true
				c.mu_reduce.Unlock()
				reply.Data[0] = ""
				reply.Data[1] = strconv.Itoa(i)
				reply.Data[2] = "1"
				// log.Printf("reduced map: %v\n", c.reduced_index)
				return nil
			}
			i++
		}
	}
	c.mu_reduce.Unlock()

	// go func() {
	// 	//	crash check
	// 	time.Sleep(time.Second * 10)
	// 	if c.current_task == 0 {
	// 		if _, err := os.Stat("mr"); errors.Is(err, os.ErrNotExist) {
	// 			// path/to/whatever does not exist
	// 		}
	// 	} else {
	// 		//	todo
	// 	}
	// }()

	elapsed := time.Since(c.start_time)
	log.Printf("Binomial took %s", elapsed)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + " world"
	// time.Sleep(time.Second * 10)

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	var files []string
	err := filepath.Walk("./", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return nil
		}
		//find the mr-out-*
		if !info.IsDir() && strings.Contains(path, "mr-out") {
			files = append(files, path)
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if len(files) == c.nReduce {
		return true
	} else {
		return false
	}

	// Your code here.

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapped_file = make(map[string]bool)
	c.reduced_index = make(map[int]bool)
	c.file_list = files
	c.nReduce = nReduce
	c.server()
	return &c
}
