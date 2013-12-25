package worker

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
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
	sync.Mutex
	agents  []*agent
	funcs   JobFuncs
	in      chan *inPack
	running bool

	Id string
	// assign a ErrFunc to handle errors
	ErrorHandler ErrorHandler
	JobHandler   JobHandler
	limit        chan bool
}

// Get a new worker
func New(limit int) (worker *Worker) {
	worker = &Worker{
		agents: make([]*agent, 0, limit),
		funcs:  make(JobFuncs),
		in:     make(chan *inPack, QUEUE_SIZE),
	}
	if limit != Unlimited {
		worker.limit = make(chan bool, limit-1)
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
	worker.agents = append(worker.agents, a)
	return
}

// Write a job to job server.
// Here, the job's mean is not the oraginal mean.
// Just looks like a network package for job's result or tell job server, there was a fail.
func (worker *Worker) broadcast(outpack *outPack) {
	for _, v := range worker.agents {
		v.write(outpack)
	}
}

// Add a function.
// Plz added job servers first, then functions.
// The API will tell every connected job server that 'I can do this'
func (worker *Worker) AddFunc(funcname string,
	f JobFunc, timeout uint32) (err error) {
	worker.Lock()
	defer worker.Unlock()
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
	outpack := getOutPack()
	if timeout == 0 {
		outpack.dataType = CAN_DO
		outpack.data = []byte(funcname)
	} else {
		outpack.dataType = CAN_DO_TIMEOUT
		l := len(funcname)
		outpack.data = getBuffer(l + 5)
		copy(outpack.data, []byte(funcname))
		outpack.data[l] = '\x00'
		binary.BigEndian.PutUint32(outpack.data[l+1:], timeout)
	}
	worker.broadcast(outpack)
}

// Remove a function.
func (worker *Worker) RemoveFunc(funcname string) (err error) {
	worker.Lock()
	defer worker.Unlock()
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
	outpack := getOutPack()
	outpack.dataType = CANT_DO
	outpack.data = []byte(funcname)
	worker.broadcast(outpack)
}

func (worker *Worker) handleInPack(inpack *inPack) {
	switch inpack.dataType {
	case NO_JOB:
		inpack.a.PreSleep()
	case NOOP:
		inpack.a.Grab()
	case ERROR:
		worker.err(GetError(inpack.data))
	case JOB_ASSIGN, JOB_ASSIGN_UNIQ:
		go func() {
			if err := worker.exec(inpack); err != nil {
				worker.err(err)
			}
		}()
		if worker.limit != nil {
			worker.limit <- true
		}
		inpack.a.Grab()
	case ECHO_RES:
		fallthrough
	default:
		worker.customeHandler(inpack)
	}
}

func (worker *Worker) Ready() (err error) {
	for _, v := range worker.agents {
		if err = v.Connect(); err != nil {
			return
		}
	}
	for funcname, f := range worker.funcs {
		worker.addFunc(funcname, f.timeout)
	}
	return
}

// Main loop
func (worker *Worker) Work() {
	worker.running = true
	for _, v := range worker.agents {
		v.Grab()
	}
	var inpack *inPack
	for inpack = range worker.in {
		worker.handleInPack(inpack)
	}
}

// job handler
func (worker *Worker) customeHandler(inpack *inPack) {
	if worker.JobHandler != nil {
		if err := worker.JobHandler(inpack); err != nil {
			worker.err(err)
		}
	}
}

// Close.
func (worker *Worker) Close() {
	worker.running = false
	close(worker.in)
}

// Send a something out, get the samething back.
func (worker *Worker) Echo(data []byte) {
	outpack := getOutPack()
	outpack.dataType = ECHO_REQ
	outpack.data = data
	worker.broadcast(outpack)
}

// Remove all of functions.
// Both from the worker or job servers.
func (worker *Worker) Reset() {
	outpack := getOutPack()
	outpack.dataType = RESET_ABILITIES
	worker.broadcast(outpack)
	worker.funcs = make(JobFuncs)
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) {
	worker.Id = id
	outpack := getOutPack()
	outpack.dataType = SET_CLIENT_ID
	outpack.data = []byte(id)
	worker.broadcast(outpack)
}

// Execute the job. And send back the result.
func (worker *Worker) exec(inpack *inPack) (err error) {
	defer func() {
		if worker.limit != nil {
			<-worker.limit
		}
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = ErrUnknown
			}
		}
	}()
	f, ok := worker.funcs[inpack.fn]
	if !ok {
		return fmt.Errorf("The function does not exist: %s", inpack.fn)
	}
	var r *result
	if f.timeout == 0 {
		d, e := f.f(inpack)
		r = &result{data: d, err: e}
	} else {
		r = execTimeout(f.f, inpack, time.Duration(f.timeout)*time.Second)
	}
	if worker.running {
		outpack := getOutPack()
		if r.err == nil {
			outpack.dataType = WORK_COMPLETE
		} else {
			if len(r.data) == 0 {
				outpack.dataType = WORK_FAIL
			} else {
				outpack.dataType = WORK_EXCEPTION
			}
			err = r.err
		}
		outpack.handle = inpack.handle
		outpack.data = r.data
		inpack.a.write(outpack)
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
