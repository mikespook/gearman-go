package client

import (
	"flag"
	"os"
	"testing"
)

const (
	TestStr = "Hello world"
)

var (
	client              *Client
	runIntegrationTests bool
)

func TestMain(m *testing.M) {
	integrationsTestFlag := flag.Bool("integration", false, "Run the integration tests (in addition to the unit tests)")
	if integrationsTestFlag != nil {
		runIntegrationTests = *integrationsTestFlag
	}
	code := m.Run()
	os.Exit(code)
}

func TestClientAddServer(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	t.Log("Add local server 127.0.0.1:4730")
	var err error
	if client, err = New(Network, "127.0.0.1:4730"); err != nil {
		t.Fatal(err)
	}
	client.ErrorHandler = func(e error) {
		t.Log(e)
	}
}

func TestClientEcho(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
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
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	handle, err := client.DoBg("ToUpper", []byte("abcdef"), JobLow)
	if err != nil {
		t.Error(err)
		return
	}
	if handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle)
	}
}

func TestClientDo(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	jobHandler := func(job *Response) {
		str := string(job.Data)
		if str == "ABCDEF" {
			t.Log(str)
		} else {
			t.Errorf("Invalid data: %s", job.Data)
		}
		return
	}
	handle, err := client.Do("ToUpper", []byte("abcdef"),
		JobLow, jobHandler)
	if err != nil {
		t.Error(err)
		return
	}
	if handle == "" {
		t.Error("Handle is empty.")
	} else {
		t.Log(handle)
	}
}

func TestClientStatus(t *testing.T) {
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
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

	handle, err := client.Do("Delay5sec", []byte("abcdef"), JobLow, nil)
	if err != nil {
		t.Error(err)
		return
	}
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
	if !runIntegrationTests {
		t.Skip("To run this test, use: go test -integration")
	}
	if err := client.Close(); err != nil {
		t.Error(err)
	}
}
