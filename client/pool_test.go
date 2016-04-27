package client

import (
	"testing"
)

var (
	pool = NewPool()
)

func TestPoolAdd(t *testing.T) {
	t.Log("Add servers")
	c := 2
	if err := pool.Add("tcp4", "127.0.0.1:4730", 1); err != nil {
		t.Fatal(err)
	}
	if err := pool.Add("tcp4", "127.0.1.1:4730", 1); err != nil {
		t.Log(err)
		c -= 1
	}
	if len(pool.Clients) != c {
		t.Errorf("%d servers expected, %d got.", c, len(pool.Clients))
	}
}

func TestPoolEcho(t *testing.T) {
	echo, err := pool.Echo("", []byte(TestStr))
	if err != nil {
		t.Error(err)
		return
	}
	if string(echo) != TestStr {
		t.Errorf("Invalid echo data: %s", echo)
		return
	}

	_, err = pool.Echo("not exists", []byte(TestStr))
	if err != ErrNotFound {
		t.Errorf("ErrNotFound expected, got %s", err)
	}
}

func TestPoolDoBg(t *testing.T) {
	addr, handle, err := pool.DoBg("ToUpper",
		[]byte("abcdef"), JobLow)
	if err != nil {
		t.Error(err)
		return
	}
	if handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(addr, handle)
	}
}

func TestPoolDo(t *testing.T) {
	jobHandler := func(job *Response) {
		str := string(job.Data)
		if str == "ABCDEF" {
			t.Log(str)
		} else {
			t.Errorf("Invalid data: %s", job.Data)
		}
		return
	}
	addr, handle, err := pool.Do("ToUpper",
		[]byte("abcdef"), JobLow, jobHandler)
	if err != nil {
		t.Error(err)
	}
	if handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(addr, handle)
	}
}

func TestPoolStatus(t *testing.T) {
	status, err := pool.Status("127.0.0.1:4730", "handle not exists")
	if err != nil {
		t.Error(err)
		return
	}
	if status.Known {
		t.Errorf("The job (%s) shouldn't be known.", status.Handle)
	}
	if status.Running {
		t.Errorf("The job (%s) shouldn't be running.", status.Handle)
	}
	addr, handle, err := pool.Do("Delay5sec",
		[]byte("abcdef"), JobLow, nil)
	if err != nil {
		t.Error(err)
		return
	}
	status, err = pool.Status(addr, handle)
	if err != nil {
		t.Error(err)
		return
	}

	if !status.Known {
		t.Errorf("The job (%s) should be known.", status.Handle)
	}
	if status.Running {
		t.Errorf("The job (%s) shouldn't be running.", status.Handle)
	}
	status, err = pool.Status("not exists", "not exists")
	if err != ErrNotFound {
		t.Error(err)
		return
	}
}

func TestPoolClose(t *testing.T) {
	return
	if err := pool.Close(); err != nil {
		t.Error(err)
	}
}
