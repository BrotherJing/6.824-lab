package mapreduce

import "fmt"

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
	tasks := make([]int, ntasks)
	for i:=0;i<ntasks;i++{
		tasks[i] = i
	}
	nComplete := 0
	var workers []string
	worker_finish := make(chan string)
	for {
		mr.Lock()
		if nComplete == ntasks{
			mr.Unlock()
			break
		}
		mr.Unlock()
		select{
		case worker := <-mr.registerChannel:
			workers = append(workers, worker)
			mr.Lock()
			if len(tasks)==0{
				mr.Unlock()
				continue
			}
			i := tasks[0]
			tasks = tasks[1:]
			mr.Unlock()
			arg := new(DoTaskArgs)
			arg.JobName = mr.jobName
			if phase == mapPhase{
				arg.File = mr.files[i]// the file to process
			}
			arg.Phase = phase // are we in mapPhase or reducePhase?
			arg.TaskNumber = i// this task's index in the current phase
			arg.NumOtherPhase = nios
			go func(){
				ok := call(worker, "Worker.DoTask", arg, new(struct{}))
				if ok{
					mr.Lock()
					nComplete += 1
					mr.Unlock()
					worker_finish <- worker
				}else{
					fmt.Printf("task %d fail!\n", arg.TaskNumber)
					mr.Lock()
					tasks = append(tasks, arg.TaskNumber)
					mr.Unlock()
				}
			}()
		case worker := <-worker_finish:
			mr.Lock()
			if nComplete == ntasks{
				mr.Unlock()
				break
			}
			mr.Unlock()
			mr.Lock()
			if len(tasks)==0{
				mr.Unlock()
				continue
			}
			i := tasks[0]
			tasks = tasks[1:]
			mr.Unlock()
			arg := new(DoTaskArgs)
			arg.JobName = mr.jobName
			if phase == mapPhase{
				arg.File = mr.files[i]// the file to process
			}
			arg.Phase = phase // are we in mapPhase or reducePhase?
			arg.TaskNumber = i// this task's index in the current phase
			arg.NumOtherPhase = nios
			go func(){
				ok := call(worker, "Worker.DoTask", arg, new(struct{}))
				if ok{
					mr.Lock()
					nComplete += 1
					mr.Unlock()
					worker_finish <- worker
				}else{
					fmt.Printf("task %d fail!\n", arg.TaskNumber)
					mr.Lock()
					tasks = append(tasks, arg.TaskNumber)
					mr.Unlock()
				}
			}()
		}
	}
	go func(){
		for _, w := range workers{
			mr.registerChannel<-w
		}
	}()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
