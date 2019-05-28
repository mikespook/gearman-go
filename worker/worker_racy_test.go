package worker

import (
	"fmt"
	"sync"
	"testing"
)

func TestWorkerRace(t *testing.T) {
	// from example worker
	// An example of worker
	w := New(Unlimited)
	defer w.Close()
	// Add a gearman job server
	if err := w.AddServer(Network, "127.0.0.1:4730"); err != nil {
		t.Fatal(err)
	}
	// A function for handling jobs
	foobar := func(job Job) ([]byte, error) {
		// Do nothing here
		return nil, nil
	}
	// Add the function to worker
	if err := w.AddFunc("foobar", foobar, 0); err != nil {
		fmt.Println(err)
		return
	}
	var wg sync.WaitGroup
	// A custome handler, for handling other results, eg. ECHO, dtError.
	w.JobHandler = func(job Job) error {
		if job.Err() == nil {
			fmt.Println(string(job.Data()))
		} else {
			fmt.Println(job.Err())
		}
		wg.Done()
		return nil
	}
	// An error handler for handling worker's internal errors.
	w.ErrorHandler = func(e error) {
		fmt.Println(e)
		// Ignore the error or shutdown the worker
	}
	// Tell Gearman job server: I'm ready!
	if err := w.Ready(); err != nil {
		fmt.Println(err)
		return
	}
	// Running main loop
	go w.Work()
	wg.Add(1)
	// calling Echo
	w.Echo([]byte("Hello"))
	// Waiting results
	wg.Wait()

	// tear down
	w.Close()
}
