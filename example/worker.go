package main

import (
    "bitbucket.org/mikespook/gearman-go/gearman"
    "bitbucket.org/mikespook/gearman-go/gearman/worker"
    "bitbucket.org/mikespook/golib/signal"
    "os"
    "fmt"
    "log"
    "strings"
)

func ToUpper(job *worker.WorkerJob) ([]byte, error) {
    data := []byte(strings.ToUpper(string(job.Data)))
    return data, nil
}

func main() {
    w := worker.New(worker.Unlimit)
    w.ErrFunc = func(e error) {
        log.Println(e)
    }
    w.AddServer("127.0.0.1:4730")
    w.AddFunction("ToUpper", ToUpper, 0)
    w.AddFunction("ToUpperTimeOut5", ToUpper, 5)

    // Catch the interrupt to exit the working loop.
    sh := signal.NewHandler()
    sh.Bind(os.Interrupt, func() bool {
        w.Close()
        return true
    })
    go sh.Loop()

    go func() {
        log.Println("start worker")
        for {
            print("cmd: ")
            var str string
            fmt.Scan(&str)
            switch str {
            case "echo":
                w.Echo([]byte("Hello world!"))
                var job *worker.WorkerJob
                for job = <-w.JobQueue; job.DataType != gearman.ECHO_RES; job = <-w.JobQueue {
                    log.Println(job)
                }
                log.Println(string(job.Data))
            case "quit":
                os.Exit(0)
                return
            case "result":
                job := <-w.JobQueue
                log.Println(string(job.Data))
            default:
                log.Println("Unknown command")
            }
        }
    }()
    w.Work()
}
