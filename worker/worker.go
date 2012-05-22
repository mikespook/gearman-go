// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
    gearman "bitbucket.org/mikespook/gearman-go"
    "bytes"
    "sync"
)

const (
    Unlimit = 0
    OneByOne = 1
)

// The definition of the callback function.
type JobFunction func(job *WorkerJob) ([]byte, error)

// Map for added function.
type JobFunctionMap map[string]JobFunction

// Error Function
type ErrFunc func(e error)
/*
Worker side api for gearman.

usage:
    w = worker.New(worker.Unlimit)
    w.AddFunction("foobar", foobar)
    w.AddServer("127.0.0.1:4730")
    w.Work() // Enter the worker's main loop

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
    limit chan bool

    JobQueue chan *WorkerJob

    // assign a ErrFunc to handle errors
    // Must assign befor AddServer
    ErrFunc ErrFunc
}

// Get a new worker
func New(l int) (worker *Worker) {
    worker = &Worker{
        // job server list
        clients: make([]*jobAgent, 0, gearman.WORKER_SERVER_CAP),
        // function list
        functions: make(JobFunctionMap),
        incoming:  make(chan *WorkerJob, gearman.QUEUE_CAP),
        JobQueue:  make(chan *WorkerJob, gearman.QUEUE_CAP),
        running:   true,
    }
    if l != Unlimit {
        worker.limit = make(chan bool, l)
        for i := 0; i < l; i ++ {
            worker.limit <- true
        }
    }
    return
}

// 
func (worker *Worker)err(e error) {
    if worker.ErrFunc != nil {
        worker.ErrFunc(e)
    }
}

// Add a server. The addr should be 'host:port' format.
// The connection is established at this time.
func (worker *Worker) AddServer(addr string) (err error) {
    worker.mutex.Lock()
    defer worker.mutex.Unlock()

    if len(worker.clients) == cap(worker.clients) {
        return gearman.ErrOutOfCap
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
        return gearman.ErrNotConn
    }
    worker.mutex.Lock()
    defer worker.mutex.Unlock()
    worker.functions[funcname] = f

    var datatype uint32
    var data []byte
    if timeout == 0 {
        datatype = gearman.CAN_DO
        data = []byte(funcname)
    } else {
        datatype = gearman.CAN_DO_TIMEOUT
        data = []byte(funcname + "\x00")
        t := gearman.Uint32ToBytes(timeout)
        data = append(data, t[:]...)
    }
    job := NewWorkerJob(gearman.REQ, datatype, data)
    worker.WriteJob(job)
    return
}

// Remove a function.
// Tell job servers 'I can not do this now' at the same time.
func (worker *Worker) RemoveFunction(funcname string) (err error) {
    worker.mutex.Lock()
    defer worker.mutex.Unlock()

    if worker.functions[funcname] == nil {
        return gearman.ErrFuncNotFound
    }
    delete(worker.functions, funcname)
    job := NewWorkerJob(gearman.REQ, gearman.CANT_DO, []byte(funcname))
    worker.WriteJob(job)
    return
}

// Main loop
func (worker *Worker) Work() {
    for _, v := range worker.clients {
        go v.Work()
    }
    for worker.running || len(worker.incoming) > 0{
        select {
        case job := <-worker.incoming:
            if job == nil {
                break
            }
            switch job.DataType {
            case gearman.NO_JOB:
                // do nothing
            case gearman.ERROR:
                _, err := gearman.GetError(job.Data)
                worker.err(err)
            case gearman.JOB_ASSIGN, gearman.JOB_ASSIGN_UNIQ:
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
    close(worker.incoming)
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
    for _, v := range worker.clients {
        err = v.Close()
    }
    worker.running = false
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
    job := NewWorkerJob(gearman.REQ, gearman.ECHO_REQ, data)
    return worker.WriteJob(job)
}

// Remove all of functions.
// Both from the worker or job servers.
func (worker *Worker) Reset() (err error) {
    job := NewWorkerJob(gearman.REQ, gearman.RESET_ABILITIES, nil)
    err = worker.WriteJob(job)
    worker.functions = make(JobFunctionMap)
    return
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) (err error) {
    job := NewWorkerJob(gearman.REQ, gearman.SET_CLIENT_ID, []byte(id))
    return worker.WriteJob(job)
}

// Execute the job. And send back the result.
func (worker *Worker) exec(job *WorkerJob) (err error) {
    if worker.limit != nil {
        <- worker.limit
        defer func() {
            worker.limit <- true
        }()
    }
    var limit int
    if job.DataType == gearman.JOB_ASSIGN {
        limit = 3
    } else {
        limit = 4
    }
    jobdata := bytes.SplitN(job.Data, []byte{'\x00'}, limit)
    job.Handle = string(jobdata[0])
    funcname := string(jobdata[1])
    if job.DataType == gearman.JOB_ASSIGN {
        job.Data = jobdata[2]
    } else {
        job.UniqueId = string(jobdata[2])
        job.Data = jobdata[3]
    }
    f, ok := worker.functions[funcname]
    if !ok {
        return gearman.ErrFuncNotFound
    }
    result, err := f(job)
    var datatype uint32
    if err == nil {
        datatype = gearman.WORK_COMPLETE
    } else {
        if result == nil {
            datatype = gearman.WORK_FAIL
        } else {
            datatype = gearman.WORK_EXCEPTION
        }
    }

    job.magicCode = gearman.REQ
    job.DataType = datatype
    job.Data = result

    worker.WriteJob(job)
    return
}
