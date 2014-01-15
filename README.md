Gearman-Go
==========

This module is a [Gearman](http://gearman.org/) API for the [Go Programming Language](http://golang.org).
The protocols were written in pure Go. It contains two sub-packages:

The client package is used for sending jobs to the Gearman job server,
and getting responses from the server.

	"github.com/mikespook/gearman-go/client"

The worker package will help developers in developing Gearman worker
service easily.

	"github.com/mikespook/gearman-go/worker"

[![Build Status](https://travis-ci.org/mikespook/gearman-go.png?branch=master)](https://travis-ci.org/mikespook/gearman-go)
[![GoDoc](https://godoc.org/github.com/mikespook/gearman-go?status.png)](https://godoc.org/github.com/mikespook/gearman-go)

Install
=======

Install the client package:

> $ go get github.com/mikespook/gearman-go/client
	
Install the worker package:

> $ go get github.com/mikespook/gearman-go/worker

Both of them:

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
	if err := w.Ready(); err != nil {
		log.Fatal(err)
		return
	}
	go w.Work()
	

## Client

	// ...
	c, err := client.New("tcp4", "127.0.0.1:4730")
    // ... error handling
	defer c.Close()
	c.ErrorHandler = func(e error) {
        log.Println(e)
    }
    echo := []byte("Hello\x00 world")
	echomsg, err := c.Echo(echo)
	// ... error handling
    log.Println(string(echomsg))
    jobHandler := func(job *client.Job) {
        log.Printf("%s", job.Data)
    }
    handle, err := c.Do("ToUpper", echo, client.JOB_NORMAL, jobHandler)
	// ...	

Branches
========

Version 0.x means: _It is far far away from stable._

__Use at your own risk!__

 * master current usable version
 * 0.2-dev Refactoring a lot of things
 * 0.1-testing Old API and some known issues, eg. [issue-14](https://github.com/mikespook/gearman-go/issues/14)

Contributors
============

(_Alphabetic order_)
 
 * [Alex Zylman](https://github.com/azylman)
 * [Ingo Oeser](https://github.com/nightlyone)
 * [jake](https://github.com/jbaikge)
 * [Jonathan Wills](https://github.com/runningwild)
 * [miraclesu](https://github.com/miraclesu)
 * [Paul Mach](https://github.com/paulmach)
 * [Sam Grimee](https://github.com/sgrimee)
 * suchj
 * [Xing Xing](http://mikespook.com) <mikespook@gmail.com> [@Twitter](http://twitter.com/mikespook)

Open Source - MIT Software License
==================================
Copyright (c) 2012 Xing Xing

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
