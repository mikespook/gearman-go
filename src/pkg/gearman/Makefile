# Copyright 2009 The Go Authors.  All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=gearman
GOFILES=\
	gearman.go\
	worker/job.go\
	worker/jobclient.go\
	worker.go\
#	client.go\

CLEANFILES+=gearman_test

include $(GOROOT)/src/Make.pkg

%: install %.go
	$(GC) $*.go
	$(LD) -o $@ $*.$O
