package main

import (
    "gearman"
    "log"
)

func main() {
    client := gearman.NewClient()
    defer client.Close()
    client.AddServer("127.0.0.1:4730")
    echo := []byte("Hello\x00world")

    if data, err := client.Echo(echo); err != nil {
        log.Println(string(data))
    }

    handle, err := client.Do("ToUpper", echo, gearman.JOB_NORMAL)
    if err != nil {
        log.Println(err)
    } else {
        log.Println(handle)
        job := <-client.JobQueue
        if data, err := job.Result(); err != nil {
            log.Println(err)
        } else {
            log.Println(string(data))
        }
    }

    known, running, numerator, denominator, err := client.Status(handle)
    if err != nil {
        log.Println(err)
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
