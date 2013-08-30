package client

import (
	"testing"
)

const (
	TestStr = "Hello world"
)

var client *Client

func TestClientAddServer(t *testing.T) {
	t.Log("Add local server 127.0.0.1:4730")
	var err error
	if client, err = New("tcp4", "127.0.0.1:4730"); err != nil {
		t.Error(err)
		return
	}
	client.ErrorHandler = func(e error) {
		t.Log(e)
	}
}

func TestClientEcho(t *testing.T) {
	echo, err := client.Echo([]byte(TestStr))
	if err != nil {
		t.Error(err)
		return
	}
	if string(echo) != TestStr {
		t.Errorf("Echo error, %s expected, %s got", TestStr, echo)
		return
	}
}

func TestClientDoBg(t *testing.T) {
	if handle := client.DoBg("ToUpper", []byte("abcdef"),
		JOB_LOW); handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle)
	}
}

func TestClientDo(t *testing.T) {
	jobHandler := func(job *Response) {
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
	status, err := client.Status("handle not exists")
	if err != nil {
		t.Error(err)
		return
	}
	if status.Known {
		t.Errorf("The job (%s) shouldn't be known.", status.Handle)
		return
	}
	if status.Running {
		t.Errorf("The job (%s) shouldn't be running.", status.Handle)
		return
	}

	handle := client.Do("Delay5sec", []byte("abcdef"), JOB_LOW, nil)
	status, err = client.Status(handle)
	if err != nil {
		t.Error(err)
		return
	}
	if !status.Known {
		t.Errorf("The job (%s) should be known.", status.Handle)
		return
	}
	if status.Running {
		t.Errorf("The job (%s) shouldn't be running.", status.Handle)
		return
	}
}

func TestClientClose(t *testing.T) {
	if err := client.Close(); err != nil {
		t.Error(err)
	}
}
