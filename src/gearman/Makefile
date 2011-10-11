# Copyright 2009 The Go Authors.  All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=gearman
GOFILES=\
	gearman.go\
	workerjob.go\
	jobagent.go\
	worker.go\
	clientjob.go\
	client.go\

CLEANFILES+=gearman_test

include $(GOROOT)/src/Make.pkg

fmt:
	gofmt -spaces=true -tabwidth=4 -w=true -tabindent=false ./
