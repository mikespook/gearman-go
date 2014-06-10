package worker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var worker *Worker

func init() {
	worker = New(Unlimited)
}

func TestWorkerErrNoneAgents(t *testing.T) {
	err := worker.Ready()
	if err != ErrNoneAgents {
		t.Error("ErrNoneAgents expected.")
	}
}

func TestWorkerAddServer(t *testing.T) {
	t.Log("Add local server 127.0.0.1:4730.")
	if err := worker.AddServer(Network, "127.0.0.1:4730"); err != nil {
		t.Error(err)
	}

	if l := len(worker.agents); l != 1 {
		t.Log(worker.agents)
		t.Error("The length of server list should be 1.")
	}
}

func TestWorkerErrNoneFuncs(t *testing.T) {
	err := worker.Ready()
	if err != ErrNoneFuncs {
		t.Error("ErrNoneFuncs expected.")
	}
}

func foobar(job Job) ([]byte, error) {
	return nil, nil
}

func TestWorkerAddFunction(t *testing.T) {
	if err := worker.AddFunc("foobar", foobar, 0); err != nil {
		t.Error(err)
	}
	if err := worker.AddFunc("timeout", foobar, 5); err != nil {
		t.Error(err)
	}
	if l := len(worker.funcs); l != 2 {
		t.Log(worker.funcs)
		t.Errorf("The length of function map should be %d.", 2)
	}
}

func TestWorkerRemoveFunc(t *testing.T) {
	if err := worker.RemoveFunc("foobar"); err != nil {
		t.Error(err)
	}
}

func TestWork(t *testing.T) {
	var wg sync.WaitGroup
	worker.JobHandler = func(job Job) error {
		t.Logf("%s", job.Data())
		wg.Done()
		return nil
	}
	if err := worker.Ready(); err != nil {
		t.Error(err)
		return
	}
	go worker.Work()
	wg.Add(1)
	worker.Echo([]byte("Hello"))
	wg.Wait()
}

func TestWorkerClose(t *testing.T) {
	worker.Close()
}

func TestWorkWithoutReady(t *testing.T) {
	other_worker := New(Unlimited)

	if err := other_worker.AddServer(Network, "127.0.0.1:4730"); err != nil {
		t.Error(err)
	}
	if err := other_worker.AddFunc("gearman-go-workertest", foobar, 0); err != nil {
		t.Error(err)
	}

	timeout := make(chan bool, 1)
	done := make(chan bool, 1)

	other_worker.JobHandler = func(j Job) error {
		if !other_worker.ready {
			t.Error("Worker not ready as expected")
		}
		done <- true
		return nil
	}
	go func() {
		time.Sleep(5 * time.Second)
		timeout <- true
	}()

	go func() {
		other_worker.Work()
	}()

	// With the all-in-one Work() we don't know if the
	// worker is ready at this stage so we may have to wait a sec:
	go func() {
		tries := 3
		for tries > 0 {
			if other_worker.ready {
				other_worker.Echo([]byte("Hello"))
				break
			}

			// still waiting for it to be ready..
			time.Sleep(1 * time.Second)
			tries--
		}
	}()

	// determine if we've finished or timed out:
	select {
	case <-timeout:
		t.Error("Test timed out waiting for the worker")
	case <-done:
	}
}

func TestWorkWithoutReadyWithPanic(t *testing.T) {
	other_worker := New(Unlimited)

	timeout := make(chan bool, 1)
	done := make(chan bool, 1)

	// Going to work with no worker setup.
	// when Work (hopefully) calls Ready it will get an error which should cause it to panic()
	go func() {
		defer func() {
			if err := recover(); err != nil {
				done <- true
				return
			}
			t.Error("Work should raise a panic.")
			done <- true
		}()
		other_worker.Work()
	}()
	go func() {
		time.Sleep(2 * time.Second)
		timeout <- true
	}()

	select {
	case <-timeout:
		t.Error("Test timed out waiting for the worker")
	case <-done:
	}

}

// initWorker creates a worker and adds the localhost server to it
func initWorker(t *testing.T) *Worker {
	otherWorker := New(Unlimited)
	if err := otherWorker.AddServer(Network, "127.0.0.1:4730"); err != nil {
		t.Error(err)
	}
	return otherWorker
}

// submitEmptyInPack sends an empty inpack with the specified fn name to the worker. It uses
// the first agent of the worker.
func submitEmptyInPack(t *testing.T, worker *Worker, function string) {
	if l := len(worker.agents); l != 1 {
		t.Error("The worker has no agents")
	}
	inpack := getInPack()
	inpack.dataType = dtJobAssign
	inpack.fn = function
	inpack.a = worker.agents[0]
	worker.in <- inpack
}

// TestShutdownSuccessJob tests that shutdown handles active jobs that will succeed
func TestShutdownSuccessJob(t *testing.T) {
	otherWorker := initWorker(t)
	finishedJob := false
	var wg sync.WaitGroup
	successJob := func(job Job) ([]byte, error) {
		wg.Done()
		// Sleep for 10ms to ensure that the shutdown waits for this to finish
		time.Sleep(time.Duration(10 * time.Millisecond))
		finishedJob = true
		return nil, nil
	}
	if err := otherWorker.AddFunc("test", successJob, 0); err != nil {
		t.Error(err)
	}
	if err := otherWorker.Ready(); err != nil {
		t.Error(err)
		return
	}
	submitEmptyInPack(t, otherWorker, "test")
	go otherWorker.Work()
	// Wait for the success_job to start so that we know we didn't shutdown before even
	// beginning to process the job.
	wg.Add(1)
	wg.Wait()
	otherWorker.Shutdown()
	if !finishedJob {
		t.Error("Didn't finish job")
	}
}

// TestShutdownFailureJob tests that shutdown handles active jobs that will fail
func TestShutdownFailureJob(t *testing.T) {
	otherWorker := initWorker(t)
	var wg sync.WaitGroup
	finishedJob := false
	failureJob := func(job Job) ([]byte, error) {
		wg.Done()
		// Sleep for 10ms to ensure that shutdown waits for this to finish
		time.Sleep(time.Duration(10 * time.Millisecond))
		finishedJob = true
		return nil, errors.New("Error!")
	}

	if err := otherWorker.AddFunc("test", failureJob, 0); err != nil {
		t.Error(err)
	}
	if err := otherWorker.Ready(); err != nil {
		t.Error(err)
		return
	}
	submitEmptyInPack(t, otherWorker, "test")
	go otherWorker.Work()
	// Wait for the failure_job to start so that we know we didn't shutdown before even
	// beginning to process the job.
	wg.Add(1)
	wg.Wait()
	otherWorker.Shutdown()
	if !finishedJob {
		t.Error("Didn't finish the failed job")
	}
}

func TestSubmitMultipleJobs(t *testing.T) {
	otherWorker := initWorker(t)
	var startJobs sync.WaitGroup
	startJobs.Add(2)
	var jobsFinished int32 = 0
	job := func(job Job) ([]byte, error) {
		startJobs.Done()
		// Sleep for 10ms to ensure that the shutdown waits for this to finish
		time.Sleep(time.Duration(10 * time.Millisecond))
		atomic.AddInt32(&jobsFinished, 1)
		return nil, nil
	}
	if err := otherWorker.AddFunc("test", job, 0); err != nil {
		t.Error(err)
	}
	if err := otherWorker.Ready(); err != nil {
		t.Error(err)
		return
	}
	submitEmptyInPack(t, otherWorker, "test")
	submitEmptyInPack(t, otherWorker, "test")
	go otherWorker.Work()
	startJobs.Wait()
	otherWorker.Shutdown()
	if jobsFinished != 2 {
		t.Error("Didn't run both jobs")
	}
}

func TestSubmitJobAfterShutdown(t *testing.T) {
	otherWorker := initWorker(t)
	noRunJob := func(job Job) ([]byte, error) {
		t.Error("This job shouldn't have been run")
		return nil, nil
	}
	if err := otherWorker.AddFunc("test", noRunJob, 0); err != nil {
		t.Error(err)
	}
	if err := otherWorker.Ready(); err != nil {
		t.Error(err)
		return
	}
	go otherWorker.Work()
	otherWorker.Shutdown()
	submitEmptyInPack(t, otherWorker, "test")
	// Sleep for 10ms to make sure that the job doesn't run
	time.Sleep(time.Duration(10 * time.Millisecond))
}
