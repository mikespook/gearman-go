package worker

import (
	"bytes"
	"sync"
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

func TestLargeDataWork(t *testing.T) {
	worker := New(Unlimited)
	defer worker.Close()

	if err := worker.AddServer(Network, "127.0.0.1:4730"); err != nil {
		t.Error(err)
	}
	worker.Ready()

	l := 5714
	var wg sync.WaitGroup

	bigdataHandler := func(job Job) error {
		defer wg.Done()
		if len(job.Data()) != l {
			t.Errorf("expected length %d. got %d.", l, len(job.Data()))
		}
		return nil
	}
	if err := worker.AddFunc("bigdata", foobar, 0); err != nil {
		defer wg.Done()
		t.Error(err)
	}

	worker.JobHandler = bigdataHandler

	worker.ErrorHandler = func(err error) {
		t.Fatal("shouldn't have received an error")
	}

	if err := worker.Ready(); err != nil {
		t.Error(err)
		return
	}
	go worker.Work()
	wg.Add(1)

	// var cli *client.Client
	// var err error
	// if cli, err = client.New(client.Network, "127.0.0.1:4730"); err != nil {
	// 	t.Fatal(err)
	// }
	// cli.ErrorHandler = func(e error) {
	// 	t.Error(e)
	// }

	// _, err = cli.Do("bigdata", bytes.Repeat([]byte("a"), l), client.JobLow, func(res *client.Response) {
	// })
	// if err != nil {
	// 	t.Error(err)
	// }

	worker.Echo(bytes.Repeat([]byte("a"), l))
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
		tries := 5
		for tries > 0 {
			if other_worker.ready {
				other_worker.Echo([]byte("Hello"))
				break
			}

			// still waiting for it to be ready..
			time.Sleep(250 * time.Millisecond)
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
