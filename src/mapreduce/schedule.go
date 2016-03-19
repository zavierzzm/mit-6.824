package mapreduce

import (
	"fmt"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup
	wg.Add(ntasks)
	// availableChannel := make(chan string, len(mr.workers))

	for i := 0; i < ntasks; i++ {
		fmt.Println(i)
		work := <-mr.registerChannel
		switch phase {
		case mapPhase:
			var args DoTaskArgs
			args.JobName = mr.jobName
			args.File = mr.files[i]
			args.Phase = mapPhase
			args.TaskNumber = i
			args.NumOtherPhase = nios
			go func(arg DoTaskArgs, worker string) {
				defer wg.Done()
				for {
					ok := call(worker, "Worker.DoTask", args, new(struct{}))
					if ok {
						go func() {
							mr.registerChannel <- worker
						}()
						break
					} else {
						worker = <-mr.registerChannel
					}
				}
			}(args, work)
		case reducePhase:
			var args DoTaskArgs
			args.JobName = mr.jobName
			args.Phase = reducePhase
			args.TaskNumber = i
			args.NumOtherPhase = nios
			go func(arg DoTaskArgs, worker string) {
				defer wg.Done()
				for {
					ok := call(worker, "Worker.DoTask", args, new(struct{}))
					if ok {
						go func() {
							mr.registerChannel <- worker
						}()
						break
					} else {
						worker = <-mr.registerChannel
					}
				}
			}(args, work)
		}
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
