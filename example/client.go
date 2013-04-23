package main

import (
    "log"
    "sync"
    "time"
    "github.com/mikespook/gearman-go/client"
)

func main() {
    var wg sync.WaitGroup
    // Set the autoinc id generator
    // You can write your own id generator 
    // by implementing IdGenerator interface.
    // client.IdGen = client.NewAutoIncId()

    c, err := client.New("127.0.0.1:4730")
    if err != nil {
        log.Fatalln(err)
    }
    defer c.Close()
    c.ErrHandler = func(e error) {
        log.Println(e)
    }
    echo := []byte("Hello\x00 world")
    wg.Add(1)
    log.Println(string(c.Echo(echo)))
    wg.Done()
    jobHandler := func(job *client.Job) {
        log.Printf("%s", job.Data)
        wg.Done()
    }
    handle := c.Do("ToUpper", echo, client.JOB_NORMAL, jobHandler)
    wg.Add(1)
    status, err := c.Status(handle, time.Second)
    if err != nil {
        log.Fatalln(err)
    }
    log.Printf("%t", status)

    wg.Wait()
}
