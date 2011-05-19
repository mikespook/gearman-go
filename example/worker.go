package main

import (
    "gearman"
    "fmt"
    "log"
    "os"
    "strings"
)

func ToUpper(job *gearman.WorkerJob) ([]byte, os.Error) {
    data := []byte(strings.ToUpper(string(job.Data)))
    return data, nil
}

func main() {
    worker := gearman.NewWorker()
    worker.AddServer("127.0.0.1:4730")
    worker.AddFunction("ToUpper", ToUpper, 0)
    worker.AddFunction("ToUpperTimeOut5", ToUpper, 5)

    go func() {
        log.Println("start worker")
        for {
            print("cmd: ")
            var str string
            fmt.Scan(&str)
            switch str {
                case "echo":
                    worker.Echo([]byte("Hello world!"))
                    job := <-worker.JobQueue
                    log.Println(string(job.Data))
                case "quit":
                    worker.Close()
                    return
                case "result":
                    job := <-worker.JobQueue
                    log.Println(string(job.Data))
                default:
                    log.Println("Unknown command")
            }
        }
    }()
    worker.Work()
}
