package client

import (
    "bitbucket.org/mikespook/gearman-go/gearman"
    "testing"
)

var client *Client

func init() {
    client = NewClient()
}

func TestClientAddServer(t *testing.T) {
    t.Log("Add local server 127.0.0.1:4730")
    if err := client.AddServer("127.0.0.1:4730"); err != nil {
        t.Error(err)
    }
}

func TestClientEcho(t *testing.T) {
    if echo, err := client.Echo([]byte("Hello world")); err != nil {
        t.Error(err)
    } else {
        t.Log(echo)
    }
}

func TestClientDo(t *testing.T) {
    if handle, err := client.Do("ToUpper", []byte("abcdef"), gearman.JOB_LOW|gearman.JOB_BG); err != nil {
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
