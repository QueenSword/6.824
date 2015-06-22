package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	var mapChan, reduceChan = make(chan int, mr.nMap), make(chan int, mr.nReduce)

	var sendMap = func(worker string, id int) bool {
		jobArgs := DoJobArgs{
			File:          mr.file,
			Operation:     Map,
			JobNumber:     id,
			NumOtherPhase: mr.nReduce,
		}
		reply := DoJobReply{}
		return call(worker, "Worker.DoJob", jobArgs, &reply)
	}

	var sendReduce = func(worker string, id int) bool {
		jobArgs := DoJobArgs{
			File:          mr.file,
			Operation:     Reduce,
			JobNumber:     id,
			NumOtherPhase: mr.nMap,
		}
		reply := DoJobReply{}
		return call(worker, "Worker.DoJob", jobArgs, &reply)

	}

	for i := 0; i < mr.nMap; i++ {
		go func(id int) {
			for {
				var worker string
				select {
				case worker = <-mr.registerChannel:
				case worker = <-mr.idleChannel:
				}
				ok := sendMap(worker, id)
				if ok {
					mapChan <- id
					mr.idleChannel <- worker
					break
				}
			}
		}(i)
	}
	for i := 0; i < mr.nMap; i++ {
		<-mapChan
	}
	fmt.Println("Map is done!")

	for i := 0; i < mr.nReduce; i++ {
		go func(id int) {
			for {
				var worker string
				select {
				case worker = <-mr.registerChannel:
				case worker = <-mr.idleChannel:
				}
				ok := sendReduce(worker, id)
				if ok {
					reduceChan <- id
					mr.idleChannel <- worker
					break
				}
			}
		}(i)
	}
	for i := 0; i < mr.nReduce; i++ {
		<-reduceChan
	}
	fmt.Println("Reduce is done!")

	return mr.KillWorkers()
}
