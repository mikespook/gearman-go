// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
    "bytes"
    "bitbucket.org/mikespook/gearman-go/common"
)

const (
    Unlimited = 0
    OneByOne = 1
)

// The definition of the callback function.
type JobFunc func(job *Job) ([]byte, error)

// Map for added function.
type JobFuncs map[string]JobFunc

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
    agents  []*jobAgent
    funcs   JobFuncs
    in chan *Job
    out chan *Job
    running bool
    limit chan bool

    Id string
    // assign a ErrFunc to handle errors
    ErrHandler common.ErrorHandler
}

// Get a new worker
func New(l int) (worker *Worker) {
    worker = &Worker{
        agents: make([]*jobAgent, 0),
        functions: make(JobFunctionMap),

        in:  make(chan *Job, common.QUEUE_SIZE),
        out:  make(chan *Job, common.QUEUE_SIZE),
        running:   true,
    }
    if l != Unlimited {
        worker.limit = make(chan bool, l)
        for i := 0; i < l; i ++ {
            worker.limit <- true
        }
    }
    go worker.outLoop()
    return
}

// 
func (worker *Worker)err(e error) {
    if worker.ErrHandler != nil {
        worker.ErrHandler(e)
    }
}

// Add a server. The addr should be 'host:port' format.
// The connection is established at this time.
func (worker *Worker) AddServer(addr string) (err error) {
    worker.mutex.Lock()
    defer worker.mutex.Unlock()

    if len(worker.clients) == cap(worker.clients) {
        return common.ErrOutOfCap
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
    f JobFunc, timeout uint32) (err error) {
    if len(worker.clients) < 1 {
        return common.ErrNotConn
    }
    worker.mutex.Lock()
    defer worker.mutex.Unlock()
    worker.functions[funcname] = f

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
    worker.WriteJob(job)
    return
}

// Remove a function.
// Tell job servers 'I can not do this now' at the same time.
func (worker *Worker) RemoveFunction(funcname string) (err error) {
    worker.mutex.Lock()
    defer worker.mutex.Unlock()

    if worker.functions[funcname] == nil {
        return common.ErrFuncNotFound
    }
    delete(worker.functions, funcname)
    job := newJob(common.REQ, common.CANT_DO, []byte(funcname))
    worker.WriteJob(job)
    return
}

// Main loop
func (worker *Worker) Work() {
    for _, v := range worker.clients {
        go v.Work()
    }
    for worker.running || len(worker.in) > 0{
        select {
        case job := <-worker.in:
            if job == nil {
                break
            }
            switch job.DataType {
            case common.NO_JOB:
                // do nothing
            case common.ERROR:
                _, err := common.GetError(job.Data)
                worker.err(err)
            case common.JOB_ASSIGN, common.JOB_ASSIGN_UNIQ:
                go func() {
                    if err := worker.exec(job); err != nil {
                        worker.err(err)
                    }
                }()
            default:
                worker.JobQueue <- job
            }
        }
    }
    close(worker.in)
}

// Close.
func (worker *Worker) Close() (err error) {
    for _, v := range worker.clients {
        err = v.Close()
    }
    worker.running = false
    return err
}

// Write a job to job server.
// Here, the job's mean is not the oraginal mean.
// Just looks like a network package for job's result or tell job server, there was a fail.
func (worker *Worker) Broadcast(job *Job) {
    for _, v := range worker.agents {
        go func() {
            if err := v.WriteJob(job); err != nil {
                worker.err(err)
            }
        }()
    }
}

// Send a something out, get the samething back.
func (worker *Worker) Echo(data []byte) (err error) {
    job := newJob(common.REQ, common.ECHO_REQ, data)
    return worker.WriteJob(job)
}

// Remove all of functions.
// Both from the worker or job servers.
func (worker *Worker) Reset() (err error) {
    job := newJob(common.REQ, common.RESET_ABILITIES, nil)
    err = worker.WriteJob(job)
    worker.functions = make(JobFunctionMap)
    return
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) (err error) {
    job := newJob(common.REQ, common.SET_CLIENT_ID, []byte(id))
    return worker.WriteJob(job)
}

// Execute the job. And send back the result.
func (worker *Worker) exec(job *Job) (err error) {
    if worker.limit != nil {
        <-worker.limit
        defer func() {
            worker.limit <- true
        }()
    }
    var limit int
    if job.DataType == common.JOB_ASSIGN {
        limit = 3
    } else {
        limit = 4
    }
    jobdata := bytes.SplitN(job.Data, []byte{'\x00'}, limit)
    job.Handle = string(jobdata[0])
    funcname := string(jobdata[1])
    if job.DataType == common.JOB_ASSIGN {
        job.Data = jobdata[2]
    } else {
        job.UniqueId = string(jobdata[2])
        job.Data = jobdata[3]
    }
    f, ok := worker.functions[funcname]
    if !ok {
        return common.ErrFuncNotFound
    }
    result, err := f(job)
    var datatype uint32
    if err == nil {
        datatype = common.WORK_COMPLETE
    } else {
        if result == nil {
            datatype = common.WORK_FAIL
        } else {
            datatype = common.WORK_EXCEPTION
        }
    }

    job.magicCode = common.REQ
    job.DataType = datatype
    job.Data = result

    worker.WriteJob(job)
    return
}
