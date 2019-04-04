package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(ntasks)

	taskChan := make(chan int, ntasks)
	for i:=0;i<ntasks;i++  {
		taskChan <- i
	}
	go func() {
		for {
			ch := <- registerChan
			go func(c string) {
				for {
					i := <- taskChan
					fmt.Printf("Schedule: job %v, worker address %v ,phase %v, task %v \n",jobName, c,phase,i)
					if call(c,"Worker.DoTask", &DoTaskArgs{jobName,
						mapFiles[i],phase,i,n_other},new(struct{})){
						waitGroup.Done()

					} else{
						taskChan <- i
					}
				}
			}(ch)
		}
	}()

	waitGroup.Wait()
	fmt.Printf("Schedule: %v %v tasks (%d I/Os) done \n", ntasks, phase, n_other)
}
