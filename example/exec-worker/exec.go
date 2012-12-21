// Copyright 2012 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a commercial
// license that can be found in the LICENSE file.

package main

import (
    "io"
    "bytes"
    "os/exec"
    "encoding/json"
    "github.com/mikespook/golib/log"
    "github.com/mikespook/gearman-go/worker"
)

type outData struct {
    Numerator, Denominator int
    Warning bool
    Data []byte
    Debug string
}

type ShExec struct {
    Name, basedir string
    Args []string
    *exec.Cmd
    job *worker.Job
    Logger *log.Logger
}

func NewShExec(basedir string, job *worker.Job) (sh *ShExec, err error) {
    sh = &ShExec{
        basedir: basedir,
        job: job,
        Args: make([]string, 0),
    }
    if err = sh.parse(job.Data); err != nil {
        return nil, err
    }
    return
}

func (sh *ShExec) parse(data []byte) (err error) {
    if err = json.Unmarshal(data, sh); err != nil {
        return
    }
    return
}

func (sh *ShExec) Append(args ... string) {
    sh.Args = append(sh.Args, args ...)
}

func (sh *ShExec) Prepend(args ... string) {
    sh.Args = append(args, sh.Args ...)
}

func (sh *ShExec) Exec() (rslt []byte, err error){
    sh.Logger.Debugf("Executing: Handle=%s, Exec=%s, Args=%v",
        sh.job.Handle, sh.Name, sh.Args)
    sh.Cmd = exec.Command(sh.Name, sh.Args ... )
    go func() {
        if ok := <-sh.job.Canceled(); ok {
            sh.Cmd.Process.Kill()
        }
    }()
    sh.Cmd.Dir = sh.basedir
    var buf bytes.Buffer
    sh.Cmd.Stdout = &buf
    var errPipe io.ReadCloser
    if errPipe, err = sh.Cmd.StderrPipe(); err != nil {
        return nil, err
    }
    defer errPipe.Close()
    go sh.processErr(errPipe)
    if err = sh.Cmd.Run(); err != nil {
        return nil, err
    }
    rslt = buf.Bytes()
    return
}

func (sh *ShExec) processErr(pipe io.ReadCloser) {
    result := make([]byte, 1024)
    var more []byte
    for {
        n, err := pipe.Read(result)
        if err != nil {
            if err != io.EOF {
                sh.job.UpdateData([]byte(err.Error()), true)
            }
            return
        }
        if more != nil {
            result = append(more, result[:n]...)
        } else {
            result = result[:n]
        }
        if n < 1024 {
            var out outData
            if err := json.Unmarshal(result, &out); err != nil {
                sh.job.UpdateData([]byte(result), true)
                return
            }
            if out.Debug == "" {
                if out.Data != nil {
                    sh.job.UpdateData(out.Data, out.Warning)
                }
                if out.Numerator != 0 || out.Denominator != 0 {
                    sh.job.UpdateStatus(out.Numerator, out.Denominator)
                }
            } else {
                sh.Logger.Debugf("Debug: Handle=%s, Exec=%s, Args=%v, Data=%s",
                sh.job.Handle, sh.Name, sh.Args, out.Debug)
            }
            more = nil
        } else {
            more = result
        }
    }
}

