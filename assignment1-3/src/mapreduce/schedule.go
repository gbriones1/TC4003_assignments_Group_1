package mapreduce

import (
	"net/rpc"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup
	for n := 0; n < ntasks; n++ {
		var args DoTaskArgs
		args.Phase = phase
		args.File = mr.files[n]
		args.JobName = mr.jobName
		args.TaskNumber = n
		args.NumOtherPhase = nios
		wg.Add(1)
		go func(){
			defer wg.Done()
			workerName := <- mr.registerChannel
			debug("Starting worker: %v for task: %v\n", workerName, args.TaskNumber)
			c, err := rpc.Dial("unix", workerName)
			checkError(err)
			// debug("Client %v\n", c)
			c.Call("Worker.DoTask", args, struct{}{})
			go func(){
				mr.registerChannel <- workerName
				debug("Registered again worker: %v\n", workerName)
			}()
		}()
	}
	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}
