// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/*
This module is Gearman API for golang. 
The protocol was implemented by native way.
*/

package gearman

import (
    "time"
    "testing"
    "strings"
    "github.com/mikespook/gearman-go/client"
    "github.com/mikespook/gearman-go/worker"
)

const(
    STR = "The gearman-go is a pure go implemented library."
    GEARMAND = "127.0.0.1:4730"
)

func ToUpper(job *worker.Job) ([]byte, error) {
    data := []byte(strings.ToUpper(string(job.Data)))
    return data, nil
}

func Sleep(job *worker.Job) ([]byte, error) {
    time.Sleep(time.Second * 5)
    return nil, nil
}


func TestJobs(t *testing.T) {
    w := worker.New(worker.Unlimited)
    if err := w.AddServer(GEARMAND); err != nil {
        t.Error(err)
        return
    }
    defer w.Close()
    if err := w.AddFunc("ToUpper", ToUpper, 0); err != nil {
        t.Error(err)
        return
    }
    if err := w.AddFunc("Sleep", Sleep, 0); err != nil {
        t.Error(err)
        return
    }

    w.ErrHandler = func(e error) {
         t.Error(e)
    }
    go w.Work()

    c, err := client.New(GEARMAND)
    if err != nil {
        t.Error(err)
        return
    }
    defer c.Close()

    c.ErrHandler = func(e error) {
        t.Error(e)
    }

    {
        jobHandler := func(job *client.Job) {
            upper := strings.ToUpper(STR)
            if (string(job.Data) != upper) {
                t.Error("%s expected, got %s", []byte(upper), job.Data)
            }
        }

        handle := c.Do("ToUpper", []byte(STR), client.JOB_NORMAL, jobHandler)
        status, err := c.Status(handle, time.Second)
        if err != nil {
            t.Error(err)
            return
        }

        if !status.Known {
            t.Errorf("%s should be known", status.Handle)
            return
        }
    }
    {
        handle := c.DoBg("Sleep", nil, client.JOB_NORMAL)
        time.Sleep(time.Second)
        status, err := c.Status(handle, time.Second)
        if err != nil {
            t.Error(err)
            return
        }

        if !status.Known {
            t.Errorf("%s should be known", status.Handle)
            return
        }

        if !status.Running {
            t.Errorf("%s should be running", status.Handle)
        }
    }
    {
        status, err := c.Status("not exists handle", time.Second)
        if err != nil {
            t.Error(err)
            return
        }

        if status.Known {
            t.Errorf("%s shouldn't be known", status.Handle)
            return
        }

        if status.Running {
            t.Errorf("%s shouldn't be running", status.Handle)
        }
    }
}
