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
    client.ErrHandler = func(e error) {
        t.Error(e)
    }
}

func TestClientEcho(t *testing.T) {
    client.JobHandler = func(job *Job) error {
        echo := string(job.Data)
        if echo == "Hello world" {
            t.Log(echo)
        } else {
            t.Errorf("Invalid echo data: %s", job.Data)
        }
        return nil
    }
    client.Echo([]byte("Hello world"))
}

func TestClientDo(t *testing.T) {
    if handle, err := client.Do("ToUpper", []byte("abcdef"), JOB_LOW|JOB_BG); err != nil {
        t.Error(err)
    } else {
        t.Log(handle)
    }
}

func TestClientClose(t *testing.T) {
    if err := client.Close(); err != nil {
        t.Error(err)
    }
}
