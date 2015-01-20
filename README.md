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

```go
// Limit number of concurrent jobs execution. 
// Use worker.Unlimited (0) if you want no limitation.
w := worker.New(worker.OneByOne)
w.ErrHandler = func(e error) {
	log.Println(e)
}
w.AddServer("127.0.0.1:4730")
// Use worker.Unlimited (0) if you want no timeout
w.AddFunc("ToUpper", ToUpper, worker.Unlimited)
// This will give a timeout of 5 seconds
w.AddFunc("ToUpperTimeOut5", ToUpper, 5)

if err := w.Ready(); err != nil {
	log.Fatal(err)
	return
}
go w.Work()
```

## Client

```go
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
jobHandler := func(resp *client.Response) {
	log.Printf("%s", resp.Data)
}
handle, err := c.Do("ToUpper", echo, client.JobNormal, jobHandler)
// ...	
```

Branches
========

Version 0.x means: _It is far far away from stable._

__Use at your own risk!__

 * master current usable version
 * 0.2-dev Refactoring a lot of things
 * 0.1-testing Old API and some known issues, eg. [issue-14](https://github.com/mikespook/gearman-go/issues/14)

Contributors
============

Great thanks to all of you for your support and interest!

(_Alphabetic order_)
 
 * [Alex Zylman](https://github.com/azylman)
 * [C.R. Kirkwood-Watts](https://github.com/kirkwood)
 * [Damian Gryski](https://github.com/dgryski)
 * [Gabriel Cristian Alecu](https://github.com/AzuraMeta)
 * [Graham Barr](https://github.com/gbarr)
 * [Ingo Oeser](https://github.com/nightlyone)
 * [jake](https://github.com/jbaikge)
 * [Joe Higton](https://github.com/draxil)
 * [Jonathan Wills](https://github.com/runningwild)
 * [Kevin Darlington](https://github.com/kdar)
 * [miraclesu](https://github.com/miraclesu)
 * [Paul Mach](https://github.com/paulmach)
 * [Randall McPherson](https://github.com/rlmcpherson)
 * [Sam Grimee](https://github.com/sgrimee)

Maintainer
==========

 * [Xing Xing](http://mikespook.com) &lt;<mikespook@gmail.com>&gt; [@Twitter](http://twitter.com/mikespook)

Open Source - MIT Software License
==================================

See LICENSE.
