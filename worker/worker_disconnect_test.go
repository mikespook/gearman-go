package worker

import (
	"log"
	"net"
	"os/exec"
	"testing"
	"time"
)

const port = `3700`

var gearman_ready chan bool
var kill_gearman chan bool
var bye chan bool

func init() {

	if check_gearman_present() {
		panic(`Something already listening on our testing port. Chickening out of testing with it!`)
	}
	gearman_ready = make( chan bool )
	kill_gearman = make( chan bool )
	// TODO: verify port is clear
	go run_gearman()
}

func run_gearman() {
	gm_cmd := exec.Command(`gearmand`, `--port`, port)
	start_err := gm_cmd.Start()

	if start_err != nil {
		panic(`could not start gearman, aborting test :` + start_err.Error())
	}
	
	// Make sure we clear up our gearman:
	defer func(){
		gm_cmd.Process.Kill()
	}()

	for tries := 10; tries > 0; tries-- {
		if check_gearman_present() {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	if !check_gearman_present() {
		panic(`Unable to start gearman aborting test`)
	}
	gearman_ready <- true
	
	<- kill_gearman
}

func check_gearman_present() bool {
	con, err := net.Dial(`tcp`, `127.0.0.1:`+port)
	if err != nil {
		log.Println("gearman not ready " + err.Error())
		return false
	}
	log.Println("gearman ready")
	con.Close()
	return true
}

func TestBasicDisconnect(t *testing.T) {
	<- gearman_ready
	
	worker := New(Unlimited)

	if err := worker.AddServer(Network, "127.0.0.1:" + port); err != nil {
		t.Error(err)
	}
	if err := worker.AddFunc("gearman-go-workertest", foobar, 0); err != nil {
		t.Error(err)
	}
	
	timeout := make(chan bool, 1)
	done := make( chan bool, 1)

	worker.JobHandler = func( j Job ) error {
		if( ! worker.ready ){
			t.Error("Worker not ready as expected");
		}
		done <-true
		return nil
	}
	handled_errors := false 
	
	c_error := make( chan bool)
	worker.ErrorHandler = func( e error ){
		handled_errors = true
		c_error <- true
	}

	go func() {
		time.Sleep(5 * time.Second)
		timeout <- true
	}()

	go func(){
		worker.Work();
	}()

	// With the all-in-one Work() we don't know if the 
	// worker is ready at this stage so we may have to wait a sec:
	go func(){
		tries := 3
		for( tries > 0 ){
			if worker.ready {
				worker.Echo([]byte("Hello"))
				kill_gearman <- true
				log.Println("ok...")
				worker.Echo([]byte("Hello"))
				break
			}

			// still waiting for it to be ready..
			time.Sleep(250 * time.Millisecond)
			tries--
		}
	}()
	

	select{
	case <- c_error:
		log.Println("eoo")
	case <- timeout:
		t.Error( "Test timed out waiting for the error handler" )
	}
}
