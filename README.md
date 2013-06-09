Gearman-Go
==========

[![Build Status](https://travis-ci.org/mikespook/gearman-go.png?branch=master)](https://travis-ci.org/mikespook/gearman-go)

This package is a [Gearman](http://gearman.org/) API for [Golang](http://golang.org).
It was implemented a native protocol for both worker and client API.

Install
=======

Install the client package:

> $ go get github.com/mikespook/gearman-go/client
	
Install the worker package:

> $ go get github.com/mikespook/gearman-go/worker

Install both:

> $ go get github.com/mikespook/gearman-go
	

Usage
=====

## Worker

    w := worker.New(worker.Unlimited)
    w.ErrHandler = func(e error) {
        log.Println(e)
    }
    w.AddServer("127.0.0.1:4730")
    w.AddFunc("ToUpper", ToUpper, worker.Immediately)
    w.AddFunc("ToUpperTimeOut5", ToUpper, 5)
    w.Work()

## Client

    c, err := client.New("127.0.0.1:4730")
    // ...
    defer c.Close()
    data := []byte("Hello\x00 world")
    c.ErrHandler = func(e error) {
        log.Println(e)
        panic(e)
    }
    jobHandler := func(job *client.Job) {
        log.Printf("%s", job.Data)
    }
    handle := c.Do("ToUpper", data, client.JOB_NORMAL, jobHandler)
    // ...

Authors
=======

 * Xing Xing <mikespook@gmail.com> [Blog](http://mikespook.com) [@Twitter](http://twitter.com/mikespook)

Open Source - MIT Software License
==================================
Copyright (c) 2012 Xing Xing

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
