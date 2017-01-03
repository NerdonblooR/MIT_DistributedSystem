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


func (mr *MapReduce) GetWorker() *WorkerInfo {
	var w *WorkerInfo
	select {
		case address:= <-mr.registerChannel:
			//use some mechanism to protect map
			mr.Workers[address] = &WorkerInfo{address}
			w = mr.Workers[address]
		case address:= <-mr.availableWorkerChannel:
			w = mr.Workers[address]
	}
	return w
}

func (mr *MapReduce) AssignJob(w *WorkerInfo, jobNum int, jt JobType) {
	//rpc call to invoke DoJob
	numOther := 0
	switch jt {
		case Map:
			numOther = mr.nReduce
		case Reduce:	
			numOther = mr.nMap
	}
	args := &DoJobArgs{mr.file, jt, jobNum, numOther}
	var reply DoJobReply
	ok := call(w.address, "Worker.DoJob", args, &reply)
	if ok == false {
		fmt.Printf("DoWork: RPC %s %s error\n", w.address, jt)
		mr.failedJobChannel <- jobNum
	}else{
		mr.jobDoneChannel <- jobNum
		mr.availableWorkerChannel <- w.address
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	var w *WorkerInfo
	//Map phase
	//add a chanel to capture failure information
	//loop an arry of unfinished jobs, when reading from failed, add to unfinished job array
	nJobDone := 0
	for mapJob :=0; mapJob < mr.nMap; mapJob++{
		w = mr.GetWorker()
		go mr.AssignJob(w, mapJob, Map)
	}

	for nJobDone < mr.nMap {
		fmt.Printf("HERE： %d\n", nJobDone)
		select {
			case <-mr.jobDoneChannel:
				nJobDone++ //just update the number of finished jobs
			case mapJob := <- mr.failedJobChannel:
				w = mr.GetWorker()
				go mr.AssignJob(w, mapJob, Map)
		}
	}

	//Reduce phase
	nJobDone = 0
	for rdcJob :=0; rdcJob < mr.nReduce; rdcJob++{
		w = mr.GetWorker()
		go mr.AssignJob(w, rdcJob, Reduce)
	}


	for nJobDone < mr.nReduce{
		select {
			case <-mr.jobDoneChannel:
				nJobDone++ //just update the number of finished jobs
			case rdcJob := <- mr.failedJobChannel:
				w = mr.GetWorker()
				go mr.AssignJob(w, rdcJob, Reduce)
		}
	}

	return mr.KillWorkers()
}






