package client

import (
	"testing"
	"time"
)

var (
	pool = NewPool()
)

func TestPoolAdd(t *testing.T) {
	t.Log("Add servers")
	if err := pool.Add("127.0.0.1:4730", 1); err != nil {
		t.Error(err)
	}
	if err := pool.Add("127.0.0.1:4730", 1); err != nil {
		t.Error(err)
	}
	if len(pool.clients) != 2 {
		t.Errorf("2 servers expected, %d got.", len(pool.clients))
	}
}

func TestPoolEcho(t *testing.T) {
	echo, err := pool.Echo("", []byte("Hello pool"), time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if string(echo) != "Hello pool" {
		t.Errorf("Invalid echo data: %s", echo)
		return
	}

	_, err = pool.Echo("not exists", []byte("Hello pool"), time.Second)
	if err != ErrNotFound {
		t.Errorf("ErrNotFound expected, got %s", err)
	}
}

func TestPoolDoBg(t *testing.T) {
	if addr, handle := pool.DoBg("ToUpper", []byte("abcdef"),
		JOB_LOW); handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(addr, handle)
	}
}

func TestPoolDo(t *testing.T) {
	jobHandler := func(job *Job) {
		str := string(job.Data)
		if str == "ABCDEF" {
			t.Log(str)
		} else {
			t.Errorf("Invalid data: %s", job.Data)
		}
		return
	}
	if addr, handle := pool.Do("ToUpper", []byte("abcdef"),
		JOB_LOW, jobHandler); handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(addr, handle)
	}
}

func TestPoolStatus(t *testing.T) {
	s1, err := pool.Status("127.0.0.1:4730", "handle not exists", time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if s1.Known {
		t.Errorf("The job (%s) shouldn't be known.", s1.Handle)
	}
	if s1.Running {
		t.Errorf("The job (%s) shouldn't be running.", s1.Handle)
	}

	addr, handle := pool.Do("Delay5sec", []byte("abcdef"), JOB_LOW, nil)
	s2, err := pool.Status(addr, handle, time.Second)
	if err != nil {
		t.Error(err)
		return
	}

	if !s2.Known {
		t.Errorf("The job (%s) should be known.", s2.Handle)
	}
	if s2.Running {
		t.Errorf("The job (%s) shouldn't be running.", s2.Handle)
	}

	_, err = pool.Status("not exists", "not exists", time.Second)
	if err != ErrNotFound {
		t.Error(err)
	}
}

func TestPoolClose(t *testing.T) {
	return
	if err := pool.Close(); err != nil {
		t.Error(err)
	}
}
