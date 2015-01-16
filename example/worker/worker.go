package main

import (
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/mikespook/gearman-go/worker"
	"github.com/mikespook/golib/signal"
)

func ToUpper(job worker.Job) ([]byte, error) {
	log.Printf("ToUpper: Data=[%s]\n", job.Data())
	data := []byte(strings.ToUpper(string(job.Data())))
	return data, nil
}

func ToUpperDelay10(job worker.Job) ([]byte, error) {
	log.Printf("ToUpper: Data=[%s]\n", job.Data())
	time.Sleep(10 * time.Second)
	data := []byte(strings.ToUpper(string(job.Data())))
	return data, nil
}

func Foobar(job worker.Job) ([]byte, error) {
	log.Printf("Foobar: Data=[%s]\n", job.Data())
	for i := 0; i < 10; i++ {
		job.SendWarning([]byte{byte(i)})
		job.SendData([]byte{byte(i)})
		job.UpdateStatus(i+1, 100)
	}
	return job.Data(), nil
}

func main() {
	log.Println("Starting ...")
	defer log.Println("Shutdown complete!")
	w := worker.New(worker.Unlimited)
	defer w.Close()
	w.ErrorHandler = func(e error) {
		log.Println(e)
		if opErr, ok := e.(*net.OpError); ok {
			if !opErr.Temporary() {
				proc, err := os.FindProcess(os.Getpid())
				if err != nil {
					log.Println(err)
				}
				if err := proc.Signal(os.Interrupt); err != nil {
					log.Println(err)
				}
			}
		}
	}
	w.JobHandler = func(job worker.Job) error {
		log.Printf("Data=%s\n", job.Data())
		return nil
	}
	w.AddServer("tcp4", "127.0.0.1:4730")
	w.AddFunc("Foobar", Foobar, worker.Unlimited)
	w.AddFunc("ToUpper", ToUpper, worker.Unlimited)
	w.AddFunc("ToUpperTimeOut5", ToUpperDelay10, 5)
	w.AddFunc("ToUpperTimeOut20", ToUpperDelay10, 20)
	w.AddFunc("SysInfo", worker.SysInfo, worker.Unlimited)
	w.AddFunc("MemInfo", worker.MemInfo, worker.Unlimited)
	if err := w.Ready(); err != nil {
		log.Fatal(err)
		return
	}
	go w.Work()
	signal.Bind(os.Interrupt, func() uint { return signal.BreakExit })
	signal.Wait()
}
