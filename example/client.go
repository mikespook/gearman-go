package main

import (
    "bitbucket.org/mikespook/gearman-go/gearman"
    "bitbucket.org/mikespook/gearman-go/gearman/client"
    "log"
)

func main() {
    client := client.New()
    defer client.Close()
    if err := client.AddServer("127.0.0.1:4730"); err != nil {
        log.Fatalln(err)
    }
    echo := []byte("Hello\x00 world")

    if data, err := client.Echo(echo); err != nil {
        log.Fatalln(string(data))
    }

    handle, err := client.Do("ToUpper", echo, gearman.JOB_NORMAL)
    if err != nil {
        log.Fatalln(err)
    } else {
        log.Println(handle)
        job := <-client.JobQueue
        if data, err := job.Result(); err != nil {
            log.Fatalln(err)
        } else {
            log.Println(string(data))
        }
    }

    known, running, numerator, denominator, err := client.Status(handle)
    if err != nil {
        log.Fatalln(err)
    }
    if !known {
        log.Println("Unknown")
    }
    if running {
        log.Printf("%g%%\n", float32(numerator)*100/float32(denominator))
    } else {
        log.Println("Not running")
    }
}
