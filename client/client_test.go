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
    }
}

func TestClientEcho(t *testing.T) {
    echoHandler = func(job *Job) {
        echo := string(job.Data)
        if echo == "Hello world" {
            t.Log(echo)
        } else {
            t.Errorf("Invalid echo data: %s", job.Data)
        }
        return
    }
    client.Echo([]byte("Hello world"), echoHandler)
}

func TestClientDoBg(t *testing.T) {
    if handle, err := client.DoBg("ToUpper", []byte("abcdef"),
        JOB_LOW); err != nil {
        t.Error(err)
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
    if handle, err := client.Do("ToUpper", []byte("abcdef"),
        JOB_LOW, jobHandler); err != nil {
        t.Error(err)
    } else {
        t.Log(handle)
    }
}

func TestClientStatus(t *testing.T) {
    statusHandler = func(handler string, known bool,
        running bool, numerator uint64, denominator uint64) {
        if known {
            t.Errorf("The job (%s) shouldn't be known.", handler)
        }
        if running {
            t.Errorf("The job (%s) shouldn't be running.", handler)
        }
    }
    client.Status("handle not exists", statusHandler)

    if handle, err := client.Do("Delay5sec", []byte("abcdef"),
        JOB_LOW, nil); err != nil {
        t.Error(err)
    } else {
        t.Log(handle)

        statusHandler = func(handler string, known bool,
            running bool, numerator uint64, denominator uint64) {
            if !known {
                t.Errorf("The job (%s) shouldn be known.", handler)
            }
            if !running {
                t.Errorf("The job (%s) shouldn be running.", handler)
            }
        }
        client.Status(handle, statusHandler)
    }
}


func TestClientClose(t *testing.T) {
    if err := client.Close(); err != nil {
        t.Error(err)
    }
}
