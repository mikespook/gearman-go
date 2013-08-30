// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"time"
	"sync"
	"encoding/binary"
)

const (
	Unlimited = 0
	OneByOne  = 1

	Immediately = 0
)

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
	agents  map[string]*agent
	funcs   JobFuncs
	in      chan *Response
	running bool
	limit   chan bool

	Id string
	// assign a ErrFunc to handle errors
	ErrorHandler ErrorHandler
	JobHandler JobHandler
	mutex sync.Mutex
}

// Get a new worker
func New(l int) (worker *Worker) {
	worker = &Worker{
		agents: make(map[string]*agent, QUEUE_SIZE),
		funcs:  make(JobFuncs),
		in:     make(chan *Response, QUEUE_SIZE),
	}
	if l != Unlimited {
		worker.limit = make(chan bool, l)
	}
	return
}

//
func (worker *Worker) err(e error) {
	if worker.ErrorHandler != nil {
		worker.ErrorHandler(e)
	}
}

// Add a server. The addr should be 'host:port' format.
// The connection is established at this time.
func (worker *Worker) AddServer(net, addr string) (err error) {
	// Create a new job server's client as a agent of server
	a, err := newAgent(net, addr, worker)
	if err != nil {
		return err
	}
	worker.agents[net + addr] = a
	return
}

// Write a job to job server.
// Here, the job's mean is not the oraginal mean.
// Just looks like a network package for job's result or tell job server, there was a fail.
func (worker *Worker) broadcast(req *request) {
	for _, v := range worker.agents {
		v.write(req)
	}
}

// Add a function.
// Plz added job servers first, then functions.
// The API will tell every connected job server that 'I can do this'
func (worker *Worker) AddFunc(funcname string,
	f JobFunc, timeout uint32) (err error) {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	if _, ok := worker.funcs[funcname]; ok {
		return fmt.Errorf("The function already exists: %s", funcname)
	}
	worker.funcs[funcname] = &jobFunc{f: f, timeout: timeout}
	if worker.running {
		worker.addFunc(funcname, timeout)
	}
	return
}

// inner add function
func (worker *Worker) addFunc(funcname string, timeout uint32) {
	req := getRequest()
	if timeout == 0 {
		req.DataType = CAN_DO
		req.Data = []byte(funcname)
	} else {
		req.DataType = CAN_DO_TIMEOUT
		l := len(funcname)
		req.Data = getBuffer(l + 5)
		copy(req.Data, []byte(funcname))
		req.Data[l] = '\x00'
		binary.BigEndian.PutUint32(req.Data[l + 1:], timeout)
	}
	worker.broadcast(req)
}

// Remove a function.
func (worker *Worker) RemoveFunc(funcname string) (err error) {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	if _, ok := worker.funcs[funcname]; !ok {
		return fmt.Errorf("The function does not exist: %s", funcname)
	}
	delete(worker.funcs, funcname)
	if worker.running {
		worker.removeFunc(funcname)
	}
	return
}

// inner remove function
func (worker *Worker) removeFunc(funcname string) {
	req := getRequest()
	req.DataType = CANT_DO
	req.Data = []byte(funcname)
	worker.broadcast(req)
}

func (worker *Worker) dealResp(resp *Response) {
	defer func() {
		if worker.running && worker.limit != nil {
			<-worker.limit
		}
	}()
	switch resp.DataType {
	case ERROR:
		worker.err(GetError(resp.Data))
	case JOB_ASSIGN, JOB_ASSIGN_UNIQ:
		if err := worker.exec(resp); err != nil {
			worker.err(err)
		}
	default:
		worker.handleResponse(resp)
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
		v.Connect()
		go v.Work()
	}
	worker.Reset()
	for funcname, f := range worker.funcs {
		worker.addFunc(funcname, f.timeout)
	}
	var resp *Response
	for resp = range worker.in {
		fmt.Println(resp)
		go worker.dealResp(resp)
	}
}

// job handler
func (worker *Worker) handleResponse(resp *Response) {
	if worker.JobHandler != nil {
		job := getJob()
		job.a = worker.agents[resp.agentId]
		job.Handle = resp.Handle
		if resp.DataType == ECHO_RES {
			job.data = resp.Data
		}
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
	req := getRequest()
	req.DataType = ECHO_REQ
	req.Data = data
	worker.broadcast(req)
}

// Remove all of functions.
// Both from the worker or job servers.
func (worker *Worker) Reset() {
	req := getRequest()
	req.DataType = RESET_ABILITIES
	worker.broadcast(req)
	worker.funcs = make(JobFuncs)
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) {
	worker.Id = id
	req := getRequest()
	req.DataType = SET_CLIENT_ID
	req.Data = []byte(id)
	worker.broadcast(req)
}

// Execute the job. And send back the result.
func (worker *Worker) exec(resp *Response) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = ErrUnknown
			}
		}
	}()
	f, ok := worker.funcs[resp.Fn]
	if !ok {
		return fmt.Errorf("The function does not exist: %s", resp.Fn)
	}
	var r *result
	job := getJob()
	job.a = worker.agents[resp.agentId]
	job.Handle = resp.Handle
	if f.timeout == 0 {
		d, e := f.f(job)
		r = &result{data: d, err: e}
	} else {
		r = execTimeout(f.f, job, time.Duration(f.timeout)*time.Second)
	}
	req := getRequest()
	if r.err == nil {
		req.DataType = WORK_COMPLETE
	} else {
		if r.data == nil {
			req.DataType = WORK_FAIL
		} else {
			req.DataType = WORK_EXCEPTION
		}
		err = r.err
	}
	req.Data = r.data
	if worker.running {
		job.a.write(req)
	}
	return
}

type result struct {
	data []byte
	err  error
}

func execTimeout(f JobFunc, job Job, timeout time.Duration) (r *result) {
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
		return &result{err: ErrTimeOut}
	}
	return r
}
