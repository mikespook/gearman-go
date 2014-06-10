// The worker package helps developers to develop Gearman's worker
// in an easy way.
package worker

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	Unlimited = iota
	OneByOne
)

// Worker is the only structure needed by worker side developing.
// It can connect to multi-server and grab jobs.
type Worker struct {
	sync.Mutex
	agents       []*agent
	funcs        jobFuncs
	in           chan *inPack
	running      bool
	ready        bool
	// The shuttingDown variable is protected by the Worker lock
	shuttingDown bool
        // Used during shutdown to wait for all active jobs to finish
	activeJobs   sync.WaitGroup

	Id           string
	ErrorHandler ErrorHandler
	JobHandler   JobHandler
	limit        chan bool
}

// Return a worker.
//
// If limit is set to Unlimited(=0), the worker will grab all jobs
// and execute them parallelly.
// If limit is greater than zero, the number of paralled executing
// jobs are limited under the number. If limit is assgined to
// OneByOne(=1), there will be only one job executed in a time.
func New(limit int) (worker *Worker) {
	worker = &Worker{
		agents: make([]*agent, 0, limit),
		funcs:  make(jobFuncs),
		in:     make(chan *inPack, queueSize),
	}
	if limit != Unlimited {
		worker.limit = make(chan bool, limit-1)
	}
	return
}

// inner error handling
func (worker *Worker) err(e error) {
	if worker.ErrorHandler != nil {
		worker.ErrorHandler(e)
	}
}

// Add a Gearman job server.
//
// addr should be formated as 'host:port'.
func (worker *Worker) AddServer(net, addr string) (err error) {
	// Create a new job server's client as a agent of server
	a, err := newAgent(net, addr, worker)
	if err != nil {
		return err
	}
	worker.agents = append(worker.agents, a)
	return
}

// Broadcast an outpack to all Gearman server.
func (worker *Worker) broadcast(outpack *outPack) {
	for _, v := range worker.agents {
		v.write(outpack)
	}
}

// Add a function.
// Set timeout as Unlimited(=0) to disable executing timeout.
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

// inner add
func (worker *Worker) addFunc(funcname string, timeout uint32) {
	outpack := getOutPack()
	if timeout == 0 {
		outpack.dataType = dtCanDo
		outpack.data = []byte(funcname)
	} else {
		outpack.dataType = dtCanDoTimeout
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

// inner remove
func (worker *Worker) removeFunc(funcname string) {
	outpack := getOutPack()
	outpack.dataType = dtCantDo
	outpack.data = []byte(funcname)
	worker.broadcast(outpack)
}

// inner package handling
func (worker *Worker) handleInPack(inpack *inPack) {
	switch inpack.dataType {
	case dtNoJob:
		inpack.a.PreSleep()
	case dtNoop:
		if !worker.isShuttingDown() {
			inpack.a.Grab()
		}
	case dtJobAssign, dtJobAssignUniq:
		go func() {
			if err := worker.exec(inpack); err != nil {
				worker.err(err)
			}
		}()
		if worker.limit != nil {
			worker.limit <- true
		}
		if !worker.isShuttingDown() {
			inpack.a.Grab()
		}
	case dtError:
		worker.err(inpack.Err())
		fallthrough
	case dtEchoRes:
		fallthrough
	default:
		worker.customeHandler(inpack)
	}
}

// Connect to Gearman server and tell every server
// what can this worker do.
func (worker *Worker) Ready() (err error) {
	if len(worker.agents) == 0 {
		return ErrNoneAgents
	}
	if len(worker.funcs) == 0 {
		return ErrNoneFuncs
	}
	for _, a := range worker.agents {
		if err = a.Connect(); err != nil {
			return
		}
	}
	for funcname, f := range worker.funcs {
		worker.addFunc(funcname, f.timeout)
	}
	worker.ready = true
	return
}

// Main loop, block here
// Most of time, this should be evaluated in goroutine.
func (worker *Worker) Work() {
	if !worker.ready {
		// didn't run Ready beforehand, so we'll have to do it:
		err := worker.Ready()
		if err != nil {
			log.Println("Error making worker ready: " + err.Error())
			panic(err)
		}
	}

	defer func() {
		for _, a := range worker.agents {
			a.Close()
		}
	}()
	worker.running = true
	for _, a := range worker.agents {
		a.Grab()
	}
	var inpack *inPack
	for inpack = range worker.in {
		worker.handleInPack(inpack)
	}
}

// custome handling warper
func (worker *Worker) customeHandler(inpack *inPack) {
	if worker.JobHandler != nil {
		if err := worker.JobHandler(inpack); err != nil {
			worker.err(err)
		}
	}
}

// Close connection and exit main loop
func (worker *Worker) Close() {
	worker.Lock()
	worker.Unlock()
	if worker.running == true {
		worker.running = false
		close(worker.in)
	}
}

// Shutdown server gracefully. This function will block until all active work has finished.
func (worker *Worker) Shutdown() {
	worker.Lock()
	worker.shuttingDown = true
	worker.Unlock()
	// Wait for all the active jobs to finish
	worker.activeJobs.Wait()
	worker.Close()
}

// Echo
func (worker *Worker) Echo(data []byte) {
	outpack := getOutPack()
	outpack.dataType = dtEchoReq
	outpack.data = data
	worker.broadcast(outpack)
}

// Remove all of functions.
// Both from the worker and job servers.
func (worker *Worker) Reset() {
	outpack := getOutPack()
	outpack.dataType = dtResetAbilities
	worker.broadcast(outpack)
	worker.funcs = make(jobFuncs)
}

// Set the worker's unique id.
func (worker *Worker) SetId(id string) {
	worker.Id = id
	outpack := getOutPack()
	outpack.dataType = dtSetClientId
	outpack.data = []byte(id)
	worker.broadcast(outpack)
}

// IsShutdown checks to see if the worker is in the process of being shutdown.
func (worker *Worker) isShuttingDown() bool {
	worker.Lock()
	defer worker.Unlock()
	return worker.shuttingDown
}

// inner job executing
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
		worker.activeJobs.Done()
	}()
	worker.activeJobs.Add(1)
	// Make sure that we don't accept any new work from old grab requests
	// after we starting shutting down.
	if worker.isShuttingDown() {
		return
	}
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
			outpack.dataType = dtWorkComplete
		} else {
			if len(r.data) == 0 {
				outpack.dataType = dtWorkFail
			} else {
				outpack.dataType = dtWorkException
			}
			err = r.err
		}
		outpack.handle = inpack.handle
		outpack.data = r.data
		inpack.a.write(outpack)
	}
	return
}

// inner result
type result struct {
	data []byte
	err  error
}

// executing timer
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
