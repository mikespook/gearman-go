package worker

import (
	"../client"
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
	gm_cmd := exec.Command(`/usr/sbin/gearmand`, `--port`, port)
	start_err := gm_cmd.Start()

	if start_err != nil {
		panic(`could not start gearman, aborting test :` + start_err.Error())
	}

	// Make sure we clear up our gearman:
	defer func() {
		log.Println("killing gearmand")
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

func check_gearman_is_dead() bool {

	for tries := 10; tries > 0; tries-- {
		if !check_gearman_present() {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

/*
 Checks for a disconnect whilst not working
*/
func TestBasicDisconnect(t *testing.T) {
	<- gearman_ready
	
	worker := New(Unlimited)
	timeout := make(chan bool, 1)
	done := make( chan bool, 1)

	if err := worker.AddServer(Network, "127.0.0.1:" + port); err != nil {
		t.Error(err)
	}
	work_done := false;
	if err := worker.AddFunc("gearman-go-workertest", 
		func(j Job)(b []byte, e error){ 
		work_done = true; 
		done <- true
		return}, 0); 
	err != nil {
		t.Error(err)
	}
	

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
		log.Println( e )
		handled_errors = true
		c_error <- true
	}

	go func() {
		time.Sleep(5 * time.Second)
		timeout <- true
	}()

	err := worker.Ready()

	if err != nil {
		t.Error(err)
	}

	go worker.Work()

	kill_gearman <- true

	check_gearman_is_dead()
	go run_gearman()

	select {
	case <-gearman_ready:
	case <-timeout:
	}

	send_client_request()

	select {
	case <- done:
		t.Error("Client request handled (somehow), did we magically reconnect?")
	case <-timeout:
		t.Error("Test timed out waiting for the error handler")
	case <-c_error:
		// error was handled!
	}

	kill_gearman <- true

}

func send_client_request(){
	log.Println("sending  client request");
	c, err := client.New( Network, "127.0.0.1:" + port )
	if err == nil {
		_, err = c.DoBg("gearman-go-workertest", []byte{}, client.JobHigh)
		if err != nil {
			log.Println( "error sending client request " + err.Error() )
		}
			
	}else{
		log.Println( "error with client " + err.Error() )
	}
}