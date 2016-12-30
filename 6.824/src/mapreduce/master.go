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
	availableWorkerChannel := make(chan string)
	var w *WorkerInfo
	//Map phase
	for mapJob :=0; mapJob < mr.nMap; mapJob++{
		select {
			case address:= <-mr.registerChannel:
				//use some mechanism to protect map
				mr.Workers[address] = &WorkerInfo{address}
				w = mr.Workers[address]
			case address:= <-availableWorkerChannel:
				w = mr.Workers[address]
		}

		go func (w *WorkerInfo,jobNum int){
			//rpc call to invoke DoJob
			args := &DoJobArgs{mr.file, Map, jobNum, mr.nReduce}
			var reply DoJobReply
			ok := call(w.address, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("DoWork: RPC %s map error\n", w.address)
			}
			availableWorkerChannel <- w.address
		}(w,mapJob)
	}

	//Reduce phase
	for rdcJob :=0; rdcJob < mr.nReduce; rdcJob++{
		select {
			case address:= <-mr.registerChannel:
				//use some mechanism to protect map
				mr.Workers[address] = &WorkerInfo{address}
				w = mr.Workers[address]
			case address:= <-availableWorkerChannel:
				w = mr.Workers[address]
		}

		go func (w *WorkerInfo,jobNum int){
			//rpc call to invoke DoJob
			args := &DoJobArgs{mr.file, Reduce, jobNum, mr.nMap}
			var reply DoJobReply
			ok := call(w.address, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("DoWork: RPC %s reduce error\n", w.address)
			}
			availableWorkerChannel <- w.address
		}(w,rdcJob)
	}

	return mr.KillWorkers()
}






