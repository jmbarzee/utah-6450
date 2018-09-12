package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
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

	taskStates := make([]taskState, ntasks)

	tasksPending := true
	for tasksPending {
		tasksPending = false
		for i, state := range taskStates {
			fmt.Printf("Checking %v \n", i)
			if state == waiting {
				tasksPending = true
				taskStates[i] = pending
				fmt.Printf("	Waiting on addr \n")
				addr := <-registerChan
				fmt.Printf("	Found addr: %s \n", addr)
				go func() {
					fileName := ""
					if phase == mapPhase {
						fileName = mapFiles[i]
					}
					args := DoTaskArgs{
						JobName:       jobName,
						File:          fileName,
						Phase:         phase,
						TaskNumber:    i,
						NumOtherPhase: n_other,
					}

					var reply struct{}

					ok := call(addr, "Worker.DoTask", args, &reply)
					if ok {
						taskStates[i] = success
						registerChan <- addr
					} else {
						taskStates[i] = waiting
					}
				}()
			} else if state == pending {
				tasksPending = true
			} else if state == success {
				// Cool! Do nothing
			}
		}
	}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}

type taskState int32

var (
	waiting = taskState(0)
	pending = taskState(1)
	success = taskState(2)
)
