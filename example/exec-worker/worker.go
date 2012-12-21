// Copyright 2012 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a commercial
// license that can be found in the LICENSE file.

package main

import (
    "os"
    "flag"
    "time"
    "github.com/mikespook/golib/log"
    "github.com/mikespook/golib/pid"
    "github.com/mikespook/golib/prof"
    "github.com/mikespook/golib/signal"
    "github.com/mikespook/gearman-go/worker"
)

var (
    pidfile = flag.String("pid", "/run/seedworker.pid",
        "PID file to write pid")
    proffile = flag.String("prof", "", "Profiling file")
    dumpfile = flag.String("dump", "", "Heap dumping file")
    dumptime = flag.Int("dumptime", 5, "Heap dumping time interval")

    joblimit = flag.Int("job-limit", worker.Unlimited,
        "Maximum number of concurrently executing job." +
        " Zero is unlimited.")

    basedir = flag.String("basedir", "", "Working directory of the php scripts.")
    timeout = flag.Uint("timeout", 30, "Executing time out.")
    gearmand = flag.String("gearmand", "127.0.0.1:4730", "Address and port of gearmand")
)

func init() {

    flag.Parse()
    initLog()

    if *basedir == "" {
        *basedir = "./script/"
    }
}

func main() {
    log.Message("Starting ... ")
    defer func() {
        time.Sleep(time.Second)
        log.Message("Shutdown complate!")
    }()

    // init profiling file
    if *proffile != "" {
        log.Debugf("Open a profiling file: %s", *proffile)
        if err := prof.Start(*proffile); err != nil {
            log.Error(err)
        } else {
            defer prof.Stop()
        }
    }

    // init heap dumping file
    if *dumpfile != "" {
        log.Debugf("Open a heap dumping file: %s", *dumpfile)
        if err := prof.NewDump(*dumpfile); err != nil {
            log.Error(err)
        } else {
            defer prof.CloseDump()
            go func() {
                for prof.Dumping {
                    time.Sleep(time.Duration(*dumptime) * time.Second)
                    prof.Dump()
                }
            }()
        }
    }

    // init pid file
    log.Debugf("Open a pid file: %s", *pidfile)
    if pidFile, err := pid.New(*pidfile); err != nil {
        log.Error(err)
    } else {
        defer pidFile.Close()
    }

    w := worker.New(*joblimit)
    if err := w.AddServer(*gearmand); err != nil {
        log.Error(err)
        return
    }
    if err := w.AddFunc("exec", execShell, uint32(*timeout)); err != nil {
        log.Error(err)
        return
    }
    if err := w.AddFunc("execphp", execPHP, uint32(*timeout)); err != nil {
        log.Error(err)
        return
    }
    defer w.Close()
    go w.Work()

    // signal handler
    sh := signal.NewHandler()
    sh.Bind(os.Interrupt, func() bool {return true})
    sh.Loop()
}
