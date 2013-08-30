// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
	"time"
)

const (
	Unlimited = 0
	OneByOne  = 1

	Immediately = 0
)

var (
	ErrConnection = common.ErrConnection
)

// Job handler
type JobHandler func(*Job) error

type JobFunc func(*Job) ([]byte, error)

// The definition of the callback function.
type jobFunc struct {
	f       JobFunc
	timeout uint32
}

// Map for added function.
type JobFuncs map[string]*jobFunc

/*
Worker side api for gearman

usage:
w = worker.New(worker.Unlimited)
w.AddFunction("foobar", foobar)
w.AddServer("127.0.0.1:4730")
w.Work() // Enter the worker's main loop

The definition of the callback function 'foobar' should suit for the type 'JobFunction'.
It looks like this:

func foobar(job *Job) (data []byte, err os.Error) {
    //sth. here
    //plaplapla...
    return
}
*/
type Worker struct {
	agents  []*agent
	funcs   JobFuncs
	in      chan *Job
	running bool
	limit   chan bool

	Id string
	// assign a ErrFunc to handle errors
	ErrHandler common.ErrorHandler
	JobHandler JobHandler
}

// Get a new worker
func New(l int) (worker *Worker) {
	worker = &Worker{
		agents: make([]*agent, 0),
		funcs:  make(JobFuncs),
		in:     make(chan *Job, common.QUEUE_SIZE),
	}
	if l != Unlimited {
		worker.limit = make(chan bool, l)
	}
	return
}

//
func (worker *Worker) err(e error) {
	if worker.ErrHandler != nil {
		worker.ErrHandler(e)
	}
}

// Add a server. The addr should be 'host:port' format.
// The connection is established at this time.
func (worker *Worker) AddServer(addr string) (err error) {
	// Create a new job server's client as a agent of server
	server, err := newAgent(addr, worker)
	if err != nil {
		return err
	}
	worker.agents = append(worker.agents, server)
	return
}

// Write a job to job server.
// Here, the job's mean is not the oraginal mean.
// Just looks like a network package for job's result or tell job server, there was a fail.
func (worker *Worker) broadcast(job *Job) {
	for _, v := range worker.agents {
		v.WriteJob(job)
	}
}

// Add a function.
// Plz added job servers first, then functions.
// The API will tell every connected job server that 'I can do this'
func (worker *Worker) AddFunc(funcname string,
	f JobFunc, timeout uint32) (err error) {
	if _, ok := worker.funcs[funcname]; ok {
		return common.Errorf("The function already exists: %s", funcname)
	}
	worker.funcs[funcname] = &jobFunc{f: f, timeout: timeout}

	if worker.running {
		worker.addFunc(funcname, timeout)
	}
	return
}

// inner add function
func (worker *Worker) addFunc(funcname string, timeout uint32) {
	var datatype uint32
	var data []byte
	if timeout == 0 {
		datatype = common.CAN_DO
		data = []byte(funcname)
	} else {
		datatype = common.CAN_DO_TIMEOUT
		data = []byte(funcname + "\x00")
		t := common.Uint32ToBytes(timeout)
		data = append(data, t[:]...)
	}
	job := newJob(common.REQ, datatype, data)
	worker.broadcast(job)

}

// Remove a function.
// Tell job servers 'I can not do this now' at the same time.
func (worker *Worker) RemoveFunc(funcname string) (err error) {
	if _, ok := worker.funcs[funcname]; !ok {
		return common.Errorf("The function does not exist: %s", funcname)
	}
	delete(worker.funcs, funcname)
	if worker.running {
		worker.removeFunc(funcname)
	}
	return
}

// inner remove function
func (worker *Worker) removeFunc(funcname string) {
	job := newJob(common.REQ, common.CANT_DO, []byte(funcname))
	worker.broadcast(job)
}

func (worker *Worker) dealJob(job *Job) {
	defer func() {
		job.Close()
		if worker.running && worker.limit != nil {
			<-worker.limit
		}
	}()
	switch job.DataType {
	case common.ERROR:
		_, err := common.GetError(job.Data)
		worker.err(err)
	case common.JOB_ASSIGN, common.JOB_ASSIGN_UNIQ:
		if err := worker.exec(job); err != nil {
			worker.err(err)
		}
	default:
		worker.handleJob(job)
	}
}

// Main loop
func (worker *Worker) Work() {
	defer func() {
		for _, v := range worker.agents {
			v.Close()
		}
	}()
	worker.running = true
	for _, v := range worker.agents {
		go v.Work()
	}
	for funcname, f := range worker.funcs {
		worker.addFunc(funcname, f.timeout)
	}
	ok := true
	for ok {
		var job *Job
		if job, ok = <-worker.in; ok {
			go worker.dealJob(job)
		}
	}
}

// job handler
func (worker *Worker) handleJob(job *Job) {
	if worker.JobHandler != nil {
		if err := worker.JobHandler(job); err != nil {
			worker.err(err)
		}
	}
}

// Close.
func (worker *Worker) Close() {
	worker.running = false
	close(worker.in)
	if worker.limit != nil {
		close(worker.limit)
	}
}

// Send a something out, get the samething back.
func (worker *Worker) Echo(data []byte) {
	job := newJob(common.REQ, common.ECHO_REQ, data)
	worker.broadcast(job)
}

// Remove all of functions.
// Both from the worker or job servers.
func (worker *Worker) Reset() {
	job := newJob(common.REQ, common.RESET_ABILITIES, nil)
	worker.broadcast(job)
	worker.funcs = make(JobFuncs)
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) {
	worker.Id = id
	job := newJob(common.REQ, common.SET_CLIENT_ID, []byte(id))
	worker.broadcast(job)
}

// Execute the job. And send back the result.
func (worker *Worker) exec(job *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = common.ErrUnknown
			}
		}
	}()
	f, ok := worker.funcs[job.Fn]
	if !ok {
		return common.Errorf("The function does not exist: %s", job.Fn)
	}
	var r *result
	if f.timeout == 0 {
		d, e := f.f(job)
		r = &result{data: d, err: e}
	} else {
		r = execTimeout(f.f, job, time.Duration(f.timeout)*time.Second)
	}
	var datatype uint32
	if r.err == nil {
		datatype = common.WORK_COMPLETE
	} else {
		if r.data == nil {
			datatype = common.WORK_FAIL
		} else {
			datatype = common.WORK_EXCEPTION
		}
		err = r.err
	}

	job.magicCode = common.REQ
	job.DataType = datatype
	job.Data = r.data
	if worker.running {
		job.agent.WriteJob(job)
	}
	return
}

func (worker *Worker) removeAgent(a *agent) {
	for k, v := range worker.agents {
		if v == a {
			worker.agents = append(worker.agents[:k], worker.agents[k+1:]...)
		}
	}
	if len(worker.agents) == 0 {
		worker.err(common.ErrNoActiveAgent)
	}
}

type result struct {
	data []byte
	err  error
}

func execTimeout(f JobFunc, job *Job, timeout time.Duration) (r *result) {
	rslt := make(chan *result)
	defer close(rslt)
	go func() {
		defer func() { recover() }()
		d, e := f(job)
		rslt <- &result{data: d, err: e}
	}()
	select {
	case r = <-rslt:
	case <-time.After(timeout):
		go job.cancel()
		return &result{err: common.ErrTimeOut}
	}
	return r
}
