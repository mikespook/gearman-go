// Copyright 2012 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a commercial
// license that can be found in the LICENSE file.

package main

import (
    "github.com/mikespook/golib/log"
    "github.com/mikespook/gearman-go/worker"
)

func execShell(job *worker.Job) (result []byte, err error) {
    log.Messagef("[Shell]Received: Handle=%s", job.Handle)
    defer log.Messagef("[Shell]Finished: Handle=%s", job.Handle)
    log.Debugf("[Shell]Received: Handle=%s, UID=%s, Data=%v", job.Handle, job.UniqueId, job.Data)
    var sh *ShExec
    if sh, err = NewShExec(*basedir, job); err != nil {
        return
    }
    sh.Logger = log.DefaultLogger
    return sh.Exec()
}

func execPHP(job *worker.Job) (result []byte, err error) {
    log.Messagef("[PHP]Received: Handle=%s", job.Handle)
    defer log.Messagef("[PHP]Finished: Handle=%s", job.Handle)
    log.Debugf("[PHP]Received: Handle=%s, UID=%s, Data=%v", job.Handle, job.UniqueId, job.Data)
    var sh *ShExec
    if sh, err = NewShExec(*basedir, job); err != nil {
        return
    }
    sh.Prepend("-f", sh.Name + ".php")
    sh.Name = "php"
    sh.Logger = log.DefaultLogger
    return sh.Exec()
}

