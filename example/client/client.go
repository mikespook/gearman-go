package main

import (
	"github.com/mikespook/gearman-go/client"
	"log"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	// Set the autoinc id generator
	// You can write your own id generator
	// by implementing IdGenerator interface.
	// client.IdGen = client.NewAutoIncId()

	c, err := client.New(client.Network, "127.0.0.1:4730")
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
	jobHandler := func(resp *client.Response) {
		log.Printf("%s", resp.Data)
		wg.Done()
	}
	handle, err := c.Do("ToUpper", echo, client.JobNormal, jobHandler)
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
