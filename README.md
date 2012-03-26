# Gearman API for golang

This module is Gearman API for golang. 
It was implemented a native protocol for both worker and client API.

Copyright 2012 Xing Xing <mikespook@gmail.com> All rights reserved.
Use of this source code is governed by a MIT license that can be found in the LICENSE file.

# INSTALL

This will install the client:

> $ go get bitbucket.org/mikespook/gearman-go/gearman/client
	
This will install the worker:

> $ go get bitbucket.org/mikespook/gearman-go/gearman/worker

This will install the client and the worker automatically:

> $ go get bitbucket.org/mikespook/gearman-go
	

# SAMPLE OF USAGE

## Worker

> $ cd example
> $ go build worker
> $ ./worker

## Client

> $ cd example
> $ go build client
> $ ./client

# Code format

> $ gofmt -spaces=true -tabwidth=4 -w=true -tabindent=false $(DIR)

# Contacts

xingxing<mikespook@gmail.com>
http://mikespook.com
http://twitter.com/mikespook
