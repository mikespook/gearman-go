package client

import (
    "time"
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
    echo, err := client.Echo([]byte("Hello world"), time.Second)
    if err != nil {
        t.Error(err)
        return
    }
    if string(echo) != "Hello world" {
        t.Errorf("Invalid echo data: %s", echo)
        return
    }
}

func TestClientDoBg(t *testing.T) {
    if handle := client.DoBg("ToUpper", []byte("abcdef"),
        JOB_LOW); handle == "" {
        t.Error("Handle is empty.")
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

    s1, err := client.Status("handle not exists", time.Second)
    if err != nil {
        t.Error(err)
        return
    }
    if s1.Known {
        t.Errorf("The job (%s) shouldn't be known.", s1.Handle)
        return
    }
    if s1.Running {
        t.Errorf("The job (%s) shouldn't be running.", s1.Handle)
        return
    }

    handle := client.Do("Delay5sec", []byte("abcdef"), JOB_LOW, nil);
    s2, err := client.Status(handle, time.Second)
    if err != nil {
        t.Error(err)
        return
    }
    if !s2.Known {
        t.Errorf("The job (%s) should be known.", s2.Handle)
        return
    }
    if s2.Running {
        t.Errorf("The job (%s) shouldn't be running.", s2.Handle)
        return
    }
}


func TestClientClose(t *testing.T) {
    if err := client.Close(); err != nil {
        t.Error(err)
    }
}
