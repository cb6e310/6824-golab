package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
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

func isExist(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

// func key(kv string) string {
// 	return strings.Split(kv, " ")[0]
// }

// func value(kv string) string {
// 	return strings.Split(kv, " ")[1]
// }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	args := MrArgs{}
	reply := MrReply{Data: make([]string, 4)}

	//for test
	// log.Printf()
	for {
		time.Sleep(time.Second)
		call("Coordinator.GetTask", &args, &reply)
		// file_index, _ := strconv.Atoi(reply.Data[1])
		file_name := reply.Data[0]
		task_ID, _ := strconv.Atoi(reply.Data[2])
		nReduce, _ := strconv.Atoi(reply.Data[3])

		// if task_ID == 1 {
		// 	break
		// }
		if task_ID == 0 {

			file, err := os.Open(file_name)
			if err != nil {
				log.Fatalf("cannot open %v", file_name)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", file_name)
			}
			file.Close()
			intermediate := mapf(file_name, string(content))
			// intermediate = append(intermediate, kva...)

			sort.Sort(ByKey(intermediate))

			//split the file by keys (words)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}

				//	create mr-X-Y name for each key
				Y := ihash(intermediate[i].Key) % nReduce
				intermediate_file_name := "mr-" + reply.Data[1] + "-" + strconv.Itoa(Y)

				//	append file operation
				if isExist(intermediate_file_name) {
					f, err := os.OpenFile(intermediate_file_name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
					if err != nil {
						panic(err)
					}

					for _, kv := range intermediate[i:j] {
						if _, err = f.WriteString(kv.Key + " " + kv.Value + "\n"); err != nil {
							panic(err)
						}
						// fmt.Printf("k:v: %v:%v\n", kv.Key, kv.Value)
					}
					f.Close()

				} else { //create file operation
					ofile, _ := os.Create(intermediate_file_name)
					for _, kv := range intermediate[i:j] {
						fmt.Fprintln(ofile, kv.Key, kv.Value)
					}
					ofile.Close()
				}

				i = j
				// fmt.Printf("now we open: %v\n", intermediate_file_name)
				// ifile, err := os.Open(intermediate_file_name)
				// if err != nil {
				// 	log.Fatal(err)
				// }
				// content, _ := ioutil.ReadAll(ifile)
				// if err := ifile.Close(); err != nil {
				// 	log.Fatal(err)
				// }

				// _content := strings.Split(string(content), "\n") //	slice of lines

				// _content = _content[:len(_content)-1] //	delete the last line
				// log.Println(intermediate_file_name)
				// log.Println(len(_content))
				// log.Println(_content[len(_content)-1])

				// bufio.NewReader(os.Stdin).ReadBytes('\n') // pause
				// bufio.NewReader(os.Stdin).ReadBytes('\n') // pause
			}

		} else {

			//task reduce

			reduce_index, _ := strconv.Atoi(reply.Data[1])
			output_name := "mr-out-" + reply.Data[1]

			var files []string
			err := filepath.Walk("./", func(path string, info os.FileInfo, err error) error {
				if err != nil {
					fmt.Println(err)
					return nil
				}

				//find the mr-X-reduce_index
				if !info.IsDir() && path[len(path)-1:] == strconv.Itoa(reduce_index) && !strings.Contains(path, "out") {
					files = append(files, path)
				}

				return nil
			})
			if err != nil {
				log.Fatal(err)
			}

			//
			//merge the *reduce_index files

			reduce_index_content := []KeyValue{}
			for _, file := range files {

				ifile, err := os.Open(file)
				if err != nil {
					log.Fatal(err)
				}
				content, _ := ioutil.ReadAll(ifile)
				if err := ifile.Close(); err != nil {
					log.Fatal(err)
				}

				_content := strings.Split(string(content), "\n") //	slice of lines

				_content = _content[:len(_content)-1] //	delete the last line

				_content_kv := []KeyValue{}
				for _, line := range _content {
					cur_kv := KeyValue{Key: strings.Split(line, " ")[0], Value: strings.Split(line, " ")[1]}
					_content_kv = append(_content_kv, cur_kv)
				}

				reduce_index_content = append(reduce_index_content, _content_kv...)

				// log.Printf("%v: %v\nlen: %v\n", output_name, reduce_index_content, len(reduce_index_content))

				// bufio.NewReader(os.Stdin).ReadBytes('\n') // pause

			}

			sort.Sort(ByKey(reduce_index_content))

			output_file, _ := os.Create(output_name)

			//
			// call Reduce on each distinct key in reduce_index_content[],
			// and print the result to mr-out-reduce_index.
			//
			i := 0
			for i < len(reduce_index_content) {
				j := i + 1
				for j < len(reduce_index_content) &&
					reduce_index_content[j].Key == reduce_index_content[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, reduce_index_content[k].Value)
				}
				output := reducef(reduce_index_content[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(output_file, "%v %v\n", reduce_index_content[i].Key, output)

				i = j
			}
			output_file.Close()

			log.Printf("mr-out-%v finished\n", reduce_index)
		}

		// bufio.NewReader(os.Stdin).ReadBytes('\n') // pause
	}

	//	for test

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = "hello"

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	// fmt.Print("reply: %v", reply.file_name)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
