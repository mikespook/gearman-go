# Gearman API for golang

This package is a [Gearman](http://gearman.org/) API for [Golang](http://golang.org).
It was implemented a native protocol for both worker and client API.

Copyright 2012 Xing Xing <mikespook@gmail.com>
All rights reserved. 
Use of this source code is governed by a MIT license that can be found 
in the LICENSE file.

# INSTALL

Install the client package:

> $ go get bitbucket.org/mikespook/gearman-go/client
	
Install the worker package:

> $ go get bitbucket.org/mikespook/gearman-go/worker

Install both:

> $ go get bitbucket.org/mikespook/gearman-go
	

# SAMPLE OF USAGE

## Worker

    w := worker.New(worker.Unlimited)
    w.ErrHandler = func(e error) {
        log.Println(e)
    }
    w.AddServer("127.0.0.1:4730")
    w.AddFunc("ToUpper", ToUpper, 0)
    w.AddFunc("ToUpperTimeOut5", ToUpper, 5)
    w.Work()

## Client

    c, err := client.New("127.0.0.1:4730")
    // ...
    defer c.Close()
    echo := []byte("Hello\x00 world")
    c.JobHandler = func(job *client.Job) error {
        log.Printf("%s", job.Data)
        return nil
    }
    c.ErrHandler = func(e error) {
        log.Println(e)
        panic(e)
    }
    handle, err := c.Do("ToUpper", echo, client.JOB_NORMAL)
    // ...

# Contacts

Xing Xing <mikespook@gmail.com>

[Blog](http://mikespook.com)

[@Twitter](http://twitter.com/mikespook)

# History

 * 0.1.1    Fixed the issue of grabbing jobs.
 * 0.1      Code refactoring; Redesign the API.
 * 0.0.1    Initial implementation, ugly code-style, slow profermance and unstable API.

# TODO
