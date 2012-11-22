// Copyright 2012 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a commercial
// license that can be found in the LICENSE file.

package main

import (
    "bitbucket.org/mikespook/golib/log"
    "flag"
    "strings"
)

var (
    logfile  = flag.String("log", "",
        "Log file to write errors and information to." +
        " Empty string output to STDOUT.")
    loglevel = flag.String("log-level", "all", "Log level to record." +
        " Values 'error', 'warning', 'message', 'debug', 'all' and 'none'" +
        " are accepted. Use '|' to combine more levels.")
)

func initLog() {
    level := log.LogNone
    levels := strings.SplitN(*loglevel, "|", -1)
    for _, v := range levels {
        switch v {
        case "none":
            level = level | log.LogNone
            break
        case "error":
            level = level | log.LogError
        case "warning":
            level = level | log.LogWarning
        case "message":
            level = level | log.LogMessage
        case "debug":
            level = level | log.LogDebug
        case "all":
            level = log.LogAll
        default:
        }
    }
    if err := log.Init(*logfile, level); err != nil {
        log.Error(err)
    }
}
