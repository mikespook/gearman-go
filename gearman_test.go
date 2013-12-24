// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/*
This module is Gearman API for golang.
The protocol was implemented by native way.
*/

package gearman

import (
	"github.com/mikespook/gearman-go/client"
	"github.com/mikespook/gearman-go/worker"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	STR      = "The gearman-go is a pure go implemented library."
	GEARMAND = "127.0.0.1:4730"
)

func ToUpper(job worker.Job) ([]byte, error) {
	data := job.Data()
	data = []byte(strings.ToUpper(string(data)))
	return data, nil
}

func Sleep(job worker.Job) ([]byte, error) {
	time.Sleep(time.Second * 5)
	return nil, nil
}

func TestJobs(t *testing.T) {
	w := worker.New(worker.Unlimited)
	if err := w.AddServer("tcp4", GEARMAND); err != nil {
		t.Error(err)
		return
	}
	defer w.Close()
	t.Log("Servers added...")
	if err := w.AddFunc("ToUpper", ToUpper, 0); err != nil {
		t.Error(err)
		return
	}
	if err := w.AddFunc("Sleep", Sleep, 0); err != nil {
		t.Error(err)
		return
	}
	t.Log("Functions added...")
	w.ErrorHandler = func(e error) {
		t.Error(e)
	}
	if err := w.Ready(); err != nil {
		t.Error(err)
		return
	}
	go w.Work()
	t.Log("Worker is running...")

	c, err := client.New("tcp4", GEARMAND)
	if err != nil {
		t.Error(err)
		return
	}
	defer c.Close()

	c.ErrorHandler = func(e error) {
		t.Log(e)
	}

	{
		var w sync.WaitGroup
		jobHandler := func(job *client.Response) {
			upper := strings.ToUpper(STR)
			if string(job.Data) != upper {
				t.Errorf("%s expected, got %s", upper, job.Data)
			}
			w.Done()
		}

		w.Add(1)
		handle, err := c.Do("ToUpper", []byte(STR), client.JOB_NORMAL, jobHandler)
		if err != nil {
			t.Error(err)
			return
		}
		w.Wait()
		status, err := c.Status(handle)
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
	{
		handle, err := c.DoBg("Sleep", nil, client.JOB_NORMAL)
		if err != nil {
			t.Error(err)
			return
		}
		time.Sleep(time.Second)
		status, err := c.Status(handle)
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
		status, err := c.Status("not exists handle")
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
