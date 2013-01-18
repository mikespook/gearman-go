package client

import (
    "errors"
    "testing"
)

var (
    pool = NewPool()
)

func TestPoolAdd(t *testing.T) {
    t.Log("Add servers")
    if err := pool.Add("127.0.0.1:4730", 1); err != nil {
        t.Error(err)
    }
    if err := pool.Add("127.0.0.2:4730", 1); err != nil {
        t.Error(err)
    }
    if len(pool.items) != 2 {
        t.Error(errors.New("2 servers expected"))
    }
}
/*
func TestPoolEcho(t *testing.T) {
    pool.JobHandler = func(job *Job) error {
        echo := string(job.Data)
        if echo == "Hello world" {
            t.Log(echo)
        } else {
            t.Errorf("Invalid echo data: %s", job.Data)
        }
        return nil
    }
    pool.Echo([]byte("Hello world"))
}
*/
/*
func TestPoolDo(t *testing.T) {
    if addr, handle, err := pool.Do("ToUpper", []byte("abcdef"), JOB_LOW|JOB_BG); err != nil {
        t.Error(err)
    } else {
        t.Log(handle)
    }
}
*/
func TestPoolClose(t *testing.T) {
    return
    if err := pool.Close(); err != nil {
        t.Error(err)
    }
}
