// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/*
This module is a Gearman API for the Go Programming Language.
The protocols were written in pure Go. It contains two sub-packages:

The client package is used for sending jobs to the Gearman job server,
and getting responses from the server.

	import "github.com/mikespook/gearman-go/client"

The worker package will help developers in developing Gearman worker
service easily.

	import "github.com/mikespook/gearman-go/worker"
*/
package gearman

import (
	_ "github.com/mikespook/gearman-go/client"
	_ "github.com/mikespook/gearman-go/worker"
)
