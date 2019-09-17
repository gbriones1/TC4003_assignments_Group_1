package mapreduce

import (
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
			ok := false
			for !ok {
				workerName := <- mr.registerChannel
				debug("Starting worker: %v for task: %v\n", workerName, args.TaskNumber)
				ok = call(workerName, "Worker.DoTask", args, nil)
				if ok {
					go func(){
						mr.registerChannel <- workerName
						debug("Registered again worker: %v\n", workerName)
					}()
				}
			}
		}()
	}
	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}
