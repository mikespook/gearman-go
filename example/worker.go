package main

import (
    "os"
    "log"
    "strings"
    "bitbucket.org/mikespook/golib/signal"
    "bitbucket.org/mikespook/gearman-go/worker"
)

func ToUpper(job *worker.Job) ([]byte, error) {
    log.Printf("Handle=[%s]; UID=[%s], Data=[%s]\n",
        job.Handle, job.UniqueId, job.Data)
    data := []byte(strings.ToUpper(string(job.Data)))
    return data, nil
}

func main() {
    log.Println("Starting ...")
    defer log.Println("Shutdown complete!")
    w := worker.New(worker.Unlimited)
    defer w.Close()
    w.ErrHandler = func(e error) {
        log.Println(e)
        if e == worker.ErrConnection {
            proc, err := os.FindProcess(os.Getpid())
            if err != nil {
                log.Println(err)
            }
            if err := proc.Signal(os.Interrupt); err != nil {
                log.Println(err)
            }
        }
    }
    w.JobHandler = func(job *worker.Job) error {
        log.Printf("H=%s, UID=%s, Data=%s\n", job.Handle,
            job.UniqueId, job.Data)
        return nil
    }
    w.AddServer("127.0.0.1:4730")
    w.AddFunc("ToUpper", ToUpper, 0)
    //w.AddFunc("ToUpperTimeOut5", ToUpper, 5)
    go w.Work()
    sh := signal.NewHandler()
    sh.Bind(os.Interrupt, func() bool {return true})
    sh.Loop()
}
