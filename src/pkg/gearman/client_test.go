package gearman

import (
    "testing"
    "os"
)

var client *Client

func init() {
    client = NewClient()
}

func TestClientAddServer(t * testing.T) {
    t.Log("Add local server 127.0.0.1:4730")
    if err := client.AddServer("127.0.0.1:4730"); err != nil {
        t.Error(err)
    }
}

func TestClientEcho(t * testing.T) {
    if err := client.Echo([]byte("Hello world")); err != nil {
        t.Error(err)
    }
}

/*
func TestClientLastResult(t * testing.T) {
    job := client.LastResult()
    if job == nil {
        t.Error(os.NewError("job shuold be the echo."))
    } else {
        t.Log(job)
    }
}
*/

func TestClientClose(t * testing.T) {
    if err := client.Close(); err != nil {
        t.Error(err)
    }
}
