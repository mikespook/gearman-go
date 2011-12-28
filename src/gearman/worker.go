// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package gearman

import (
	"bytes"
	"errors"
	"sync"
)

// The definition of the callback function.
type JobFunction func(job *WorkerJob) ([]byte, error)
// Map for added function.
type JobFunctionMap map[string]JobFunction

/*
Worker side api for gearman.

usage:
    worker = NewWorker()
    worker.AddFunction("foobar", foobar)
    worker.AddServer("127.0.0.1:4730")
    worker.Work() // Enter the worker's main loop

The definition of the callback function 'foobar' should suit for the type 'JobFunction'.
It looks like this:

func foobar(job *WorkerJob) (data []byte, err os.Error) {
    //sth. here
    //plaplapla...
    return
}
*/
type Worker struct {
	clients   []*jobAgent
	functions JobFunctionMap

	running  bool
	incoming chan *WorkerJob
	mutex    sync.Mutex
	JobQueue chan *WorkerJob
	ErrQueue chan error
}

// Get a new worker
func NewWorker() (worker *Worker) {
	worker = &Worker{
		// job server list
		clients: make([]*jobAgent, 0, WORKER_SERVER_CAP),
		// function list
		functions: make(JobFunctionMap),
		incoming:  make(chan *WorkerJob, QUEUE_CAP),
		JobQueue:  make(chan *WorkerJob, QUEUE_CAP),
		ErrQueue:  make(chan error, QUEUE_CAP),
		running:   true,
	}
	return
}

// Add a server. The addr should be 'host:port' format.
// The connection is established at this time.
func (worker *Worker) AddServer(addr string) (err error) {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	if len(worker.clients) == cap(worker.clients) {
		return errors.New("To many servers added.")
	}

	// Create a new job server's client as a agent of server
	server, err := newJobAgent(addr, worker)
	if err != nil {
		return err
	}

	n := len(worker.clients)
	worker.clients = worker.clients[0 : n+1]
	worker.clients[n] = server
	return
}

// Add a function.
// Plz added job servers first, then functions.
// The API will tell every connected job server that 'I can do this'
func (worker *Worker) AddFunction(funcname string,
	f JobFunction, timeout uint32) (err error) {
	if len(worker.clients) < 1 {
		return errors.New("Did not connect to Job Server.")
	}
	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	worker.functions[funcname] = f

	var datatype uint32
	var data []byte
	if timeout == 0 {
		datatype = CAN_DO
		data = []byte(funcname)
	} else {
		datatype = CAN_DO_TIMEOUT
		data = []byte(funcname + "\x00")
		t := uint32ToByte(timeout)
		data = append(data, t[:]...)
	}
	job := NewWorkerJob(REQ, datatype, data)
	worker.WriteJob(job)
	return
}

// Remove a function.
// Tell job servers 'I can not do this now' at the same time.
func (worker *Worker) RemoveFunction(funcname string) (err error) {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	if worker.functions[funcname] == nil {
		return errors.New("No function named: " + funcname)
	}
	delete(worker.functions, funcname)
	job := NewWorkerJob(REQ, CANT_DO, []byte(funcname))
	worker.WriteJob(job)
	return
}

// Main loop
func (worker *Worker) Work() {
	for _, v := range worker.clients {
		go v.Work()
	}
	for worker.running {
		select {
		case job := <-worker.incoming:
			if job == nil {
				break
			}
			switch job.DataType {
			case NO_JOB:
				// do nothing
			case ERROR:
				_, err := getError(job.Data)
				worker.ErrQueue <- err
			case JOB_ASSIGN, JOB_ASSIGN_UNIQ:
				go func() {
					if err := worker.exec(job); err != nil {
						worker.ErrQueue <- err
					}
				}()
			default:
				worker.JobQueue <- job
			}
		}
	}
}

// Get the last job in queue.
// If there are more than one job in the queue, 
// the last one will be returned,
// the others will be lost.
func (worker *Worker) LastJob() (job *WorkerJob) {
	if l := len(worker.JobQueue); l != 1 {
		if l == 0 {
			return
		}
		for i := 0; i < l-1; i++ {
			<-worker.JobQueue
		}
	}
	return <-worker.JobQueue
}

// Close.
func (worker *Worker) Close() (err error) {
	worker.running = false
	for _, v := range worker.clients {
		err = v.Close()
	}
	close(worker.incoming)
	return err
}

// Write a job to job server.
// Here, the job's mean is not the oraginal mean.
// Just looks like a network package for job's result or tell job server, there was a fail.
func (worker *Worker) WriteJob(job *WorkerJob) (err error) {
	e := make(chan error)
	for _, v := range worker.clients {
		go func() {
			e <- v.WriteJob(job)
		}()
	}
	return <-e
}

// Send a something out, get the samething back.
func (worker *Worker) Echo(data []byte) (err error) {
	job := NewWorkerJob(REQ, ECHO_REQ, data)
	return worker.WriteJob(job)
}

// Remove all of functions.
// Both from the worker or job servers.
func (worker *Worker) Reset() (err error) {
	job := NewWorkerJob(REQ, RESET_ABILITIES, nil)
	err = worker.WriteJob(job)
	worker.functions = make(JobFunctionMap)
	return
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) (err error) {
	job := NewWorkerJob(REQ, SET_CLIENT_ID, []byte(id))
	return worker.WriteJob(job)
}

// Execute the job. And send back the result.
func (worker *Worker) exec(job *WorkerJob) (err error) {
	var limit int
	if job.DataType == JOB_ASSIGN {
		limit = 3
	} else {
		limit = 4
	}
	jobdata := bytes.SplitN(job.Data, []byte{'\x00'}, limit)
	job.Handle = string(jobdata[0])
	funcname := string(jobdata[1])
	if job.DataType == JOB_ASSIGN {
		job.Data = jobdata[2]
	} else {
		job.UniqueId = string(jobdata[2])
		job.Data = jobdata[3]
	}
	f := worker.functions[funcname]
	if f == nil {
		return errors.New("function is nil")
	}
	result, err := f(job)
	var datatype uint32
	if err == nil {
		datatype = WORK_COMPLETE
	} else {
		if result == nil {
			datatype = WORK_FAIL
		} else {
			datatype = WORK_EXCEPTION
		}
	}

	job.magicCode = REQ
	job.DataType = datatype
	job.Data = result

	worker.WriteJob(job)
	return
}
