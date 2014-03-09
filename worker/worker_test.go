package worker

import (
	"bytes"
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
