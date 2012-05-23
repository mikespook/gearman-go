package main

import (
    "bitbucket.org/mikespook/gearman-go/worker"
//    "bitbucket.org/mikespook/golib/signal"
//    "os"
    "log"
    "strings"
)

func ToUpper(job *worker.Job) ([]byte, error) {
    data := []byte(strings.ToUpper(string(job.Data)))
    return data, nil
}

func main() {
    w := worker.New(worker.Unlimited)
    w.ErrHandler = func(e error) {
        log.Println(e)
    }
    w.AddServer("127.0.0.1:4730")
    w.AddFunction("ToUpper", ToUpper, 0)
    w.AddFunction("ToUpperTimeOut5", ToUpper, 5)
    w.Work()
}
