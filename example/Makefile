# Copyright 2009 The Go Authors.  All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=gearman

# GOFILES=\
#	worker.go\

CLEANFILES+= worker client

include $(GOROOT)/src/Make.pkg

%: %.go
	$(GC) $*.go
	$(LD) -o $@ $*.$O

fmt:
	gofmt -spaces=true -tabwidth=4 -w=true -tabindent=false ./
