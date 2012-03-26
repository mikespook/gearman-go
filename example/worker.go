package main

import (
    "bitbucket.org/mikespook/gearman-go/gearman"
    "bitbucket.org/mikespook/gearman-go/gearman/worker"
    "fmt"
    "log"
    "strings"
)

func ToUpper(job *worker.WorkerJob) ([]byte, error) {
    data := []byte(strings.ToUpper(string(job.Data)))
    return data, nil
}

func main() {
    w := worker.NewWorker()
    defer w.Close()
    w.AddServer("127.0.0.1:4730")
    w.AddFunction("ToUpper", ToUpper, 0)
    w.AddFunction("ToUpperTimeOut5", ToUpper, 5)

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
