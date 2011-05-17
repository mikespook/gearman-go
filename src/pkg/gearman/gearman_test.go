package gearman

import (
    "testing"
)

var worker *Worker

func init() {
    worker = NewWorker()
}

func TestAddServer(t *testing.T) {
    t.Log("Add local server 127.0.0.1:4730.")
    if err := worker.AddServer("127.0.0.1:4730"); err != nil {
        t.Error(err)
    }
    
    if l := len(worker.servers); l != 1 {
        t.Log(worker.servers)
        t.Error("The length of server list should be 1.")
    }
}

func TestAddFunction(t *testing.T) {
    f := func(job *Job) []byte {
        return nil
    }

    if err := worker.AddFunction("foobar", f); err != nil {
        t.Error(err)
    }
    if l := len(worker.functions); l != 1 {
        t.Log(worker.functions)
        t.Error("The length of function map should be 1.")
    }
}

func TestEcho(t * testing.T) {
    go worker.Work()
    if err := worker.Echo([]byte("Hello World")); err != nil {
        t.Error(err)
    }
}

func TestClose(t *testing.T) {
    if err := worker.Close(); err != nil {
        t.Error(err)
    }
}
