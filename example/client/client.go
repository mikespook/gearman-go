package main

import (
    "log"
    "sync"
    "github.com/mikespook/gearman-go/client"
)

func main() {
    var wg sync.WaitGroup
    // Set the autoinc id generator
    // You can write your own id generator 
    // by implementing IdGenerator interface.
    // client.IdGen = client.NewAutoIncId()

    c, err := client.New("tcp4", "127.0.0.1:4730")
    if err != nil {
        log.Fatalln(err)
    }
    defer c.Close()
    c.ErrorHandler = func(e error) {
        log.Println(e)
    }
    echo := []byte("Hello\x00 world")
    wg.Add(1)
    echomsg, err := c.Echo(echo)
    if err != nil {
        log.Fatalln(err)
    }
    log.Println(string(echomsg))
    wg.Done()
    jobHandler := func(job *client.Response) {
        log.Printf("%s", job.Data)
        wg.Done()
    }
    handle, err := c.Do("ToUpper", echo, client.JOB_NORMAL, jobHandler)
	if err != nil {
		log.Fatalln(err)
	}
    wg.Add(1)
    status, err := c.Status(handle)
    if err != nil {
        log.Fatalln(err)
    }
    log.Printf("%t", status)

    wg.Wait()
}
