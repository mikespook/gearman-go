package client

import (
    "testing"
)

var client *Client

func TestClientAddServer(t *testing.T) {
    t.Log("Add local server 127.0.0.1:4730")
    var err error
    if client, err = New("127.0.0.1:4730"); err != nil {
        t.Error(err)
        return
    }
    client.ErrHandler = func(e error) {
        t.Log(e)
    }
}

func TestClientEcho(t *testing.T) {
    if echo := string(client.Echo([]byte("Hello world"))); echo == "Hello world" {
        t.Log(echo)
    } else {
        t.Errorf("Invalid echo data: %s", echo)
    }
}

func TestClientDoBg(t *testing.T) {
    if handle := client.DoBg("ToUpper", []byte("abcdef"),
        JOB_LOW); handle == "" {
        t.Error("Handle is empty.")
    } else {
        t.Log(handle)
    }
}

func TestClientDo(t *testing.T) {
    jobHandler := func(job *Job) {
        str := string(job.Data)
        if str == "ABCDEF" {
            t.Log(str)
        } else {
            t.Errorf("Invalid data: %s", job.Data)
        }
        return
    }
    if handle := client.Do("ToUpper", []byte("abcdef"),
        JOB_LOW, jobHandler); handle == "" {
        t.Error("Handle is empty.")
    } else {
        t.Log(handle)
    }
}

func TestClientStatus(t *testing.T) {

    s1 := client.Status("handle not exists")
    if s1.Known {
        t.Errorf("The job (%s) shouldn't be known.", s1.Handle)
    }
    if s1.Running {
        t.Errorf("The job (%s) shouldn't be running.", s1.Handle)
    }

    handle := client.Do("Delay5sec", []byte("abcdef"), JOB_LOW, nil);
    s2 := client.Status(handle)
    if !s2.Known {
        t.Errorf("The job (%s) should be known.", s2.Handle)
    }
    if s2.Running {
        t.Errorf("The job (%s) shouldn't be running.", s2.Handle)
    }
}


func TestClientClose(t *testing.T) {
    if err := client.Close(); err != nil {
        t.Error(err)
    }
}
