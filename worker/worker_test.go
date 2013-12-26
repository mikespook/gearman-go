package worker

import (
	"sync"
	"testing"
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
