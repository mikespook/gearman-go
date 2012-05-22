package main

import (
    "log"
    "sync"
    "bitbucket.org/mikespook/gearman-go"
    "bitbucket.org/mikespook/gearman-go/client"
)

func main() {
    var wg sync.WaitGroup

    c, err := client.New("tcp4", "127.0.0.1:4730")
    if err != nil {
        log.Fatalln(err)
    }
    defer c.Close()
    echo := []byte("Hello\x00 world")
    c.JobHandler = func(job *client.Job) error {
        log.Printf("%V", job)
        wg.Done()
        return nil
    }

    wg.Add(1)
    c.Echo(echo)

    handle, err := c.Do("ToUpper", echo, gearman.JOB_NORMAL)
    if err != nil {
        log.Fatalln(err)
    } else {
        log.Println(handle)
    }

    c.StatusHandler = func(handle string, known, running bool, numerator, denominator uint64) {
        log.Printf("%s: %b, %b, %d, %d", handle, known, running, numerator, denominator)
        wg.Done()
    }
    wg.Add(1)
    c.Status(handle)
    wg.Wait()
}
