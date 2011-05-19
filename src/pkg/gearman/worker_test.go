package gearman

import (
    "testing"
    "os"
)

var worker *Worker

func init() {
    worker = NewWorker()
}

func TestWorkerAddServer(t *testing.T) {
    t.Log("Add local server 127.0.0.1:4730.")
    if err := worker.AddServer("127.0.0.1:4730"); err != nil {
        t.Error(err)
    }

    if l := len(worker.clients); l != 1 {
        t.Log(worker.clients)
        t.Error("The length of server list should be 1.")
    }
}

func foobar(job *WorkerJob) ([]byte, os.Error) {
    return nil, nil
}


func TestWorkerAddFunction(t *testing.T) {
    if err := worker.AddFunction("foobar", foobar, 0); err != nil {
        t.Error(err)
    }
    if err := worker.AddFunction("timeout", foobar, 5); err != nil {
        t.Error(err)
    }
    if l := len(worker.functions); l != 2 {
        t.Log(worker.functions)
        t.Errorf("The length of function map should be %d.", 2)
    }
}

func TestWorkerEcho(t * testing.T) {
    if err := worker.Echo([]byte("Hello World")); err != nil {
        t.Error(err)
    }
}
/*
func TestWorkerResult(t *testing.T) {
    if job := worker.LastResult(); job == nil {
        t.Error("Nothing in result.")
    } else {
        t.Log(job)
    }
}
*/

func TestWorkerRemoveFunction(t * testing.T) {
    if err := worker.RemoveFunction("foobar"); err != nil {
        t.Error(err)
    }
}

func TestWorkerReset(t * testing.T) {
    if err := worker.Reset(); err != nil {
        t.Error(err)
    }
}

func TestWorkerClose(t *testing.T) {
    if err := worker.Close(); err != nil {
        t.Error(err)
    }
}
