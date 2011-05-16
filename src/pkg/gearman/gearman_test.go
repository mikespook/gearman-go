package gearman

import (
    "testing"
)

func TestAddServer(t *testing.T) {
    worker := NewWorker()
    t.Log("Add local server 127.0.0.1:4730.")
    if err := worker.AddServer("127.0.0.1:4730"); err != nil {
        t.Error(err)
    }
    
    if l := len(worker.servers); l != 1 {
        t.Log(worker.servers)
        t.Error("The length of server list should be 1.")
    }
}
