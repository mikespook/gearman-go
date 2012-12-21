// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
    "io"
    "net"
    "github.com/mikespook/gearman-go/common"
)

// The agent of job server.
type agent struct {
    conn        net.Conn
    worker      *Worker
    in chan     []byte
    out chan    *Job
    addr string
}

// Create the agent of job server.
func newAgent(addr string, worker *Worker) (a *agent, err error) {
    conn, err := net.Dial(common.NETWORK, addr)
    if err != nil {
        return
    }
    a = &agent{
        conn: conn,
        worker: worker,
        addr: addr,
        in: make(chan []byte, common.QUEUE_SIZE),
        out: make(chan *Job, common.QUEUE_SIZE),
    }
    // reset abilities
    a.WriteJob(newJob(common.REQ, common.RESET_ABILITIES, nil))
    return
}

// outputing loop
func (a *agent) outLoop() {
    ok := true
    var job *Job
    for ok {
        if job, ok = <-a.out; ok {
            if err := a.write(job.Encode()); err != nil {
                a.worker.err(err)
            }
        }
    }
}

// inputing loop
func (a *agent) inLoop() {
    defer func() {
        if r := recover(); r != nil {
            a.worker.err(common.Errorf("Exiting: %s", r))
        }
        close(a.in)
        close(a.out)
        a.worker.removeAgent(a)
    }()
    for a.worker.running {
        a.WriteJob(newJob(common.REQ, common.PRE_SLEEP, nil))
        RESTART:
        // got noop msg and in queue is zero, grab job
        rel, err := a.read()
        if err != nil {
            if err == common.ErrConnection {
                for i:= 0; i < 3 && a.worker.running; i++ {
                    if conn, err := net.Dial(common.NETWORK, a.addr); err != nil {
                        a.worker.err(common.Errorf("Reconnection: %d faild", i))
                        continue
                    } else {
                        a.conn = conn
                        goto RESTART
                    }
                }
                a.worker.err(err)
                break
            }
            a.worker.err(err)
            continue
        }
        job, err := decodeJob(rel)
        if err != nil {
            a.worker.err(err)
            continue
        }
        switch job.DataType {
        case common.NOOP:
            a.WriteJob(newJob(common.REQ, common.GRAB_JOB_UNIQ, nil))
        case common.ERROR, common.ECHO_RES, common.JOB_ASSIGN_UNIQ, common.JOB_ASSIGN:
            job.agent = a
            a.worker.in <- job
        }
    }
}

func (a *agent) Close() {
    a.conn.Close()
}

func (a *agent) Work() {
    go a.outLoop()
    go a.inLoop()
}

// Internal read
func (a *agent) read() (data []byte, err error) {
    if len(a.in) > 0 {
        // in queue is not empty
        data = <-a.in
    } else {
        for {
            buf := make([]byte, common.BUFFER_SIZE)
            var n int
            if n, err = a.conn.Read(buf); err != nil {
                if err == io.EOF && n == 0 {
                    if data == nil {
                        err = common.ErrConnection
                        return
                    }
                    break
                }
                return
            }
            data = append(data, buf[0:n]...)
            if n < common.BUFFER_SIZE {
                break
            }
        }
    }
    // split package
    tl := len(data)
    start := 0
    for i := 0; i < tl; i++ {
        if string(data[start:start+4]) == common.RES_STR {
            l := int(common.BytesToUint32([4]byte{data[start+8],
                data[start+9], data[start+10], data[start+11]}))
            total := l + 12
            if total == tl {
                return
            } else {
                a.in <- data[total:]
                data = data[:total]
                return
            }
        } else {
            start++
        }
    }
    return nil, common.Errorf("Invalid data: %V", data)
}

// Send a job to the job server.
func (a *agent) WriteJob(job *Job) {
    a.out <- job
}

// Internal write the encoded job.
func (a *agent) write(buf []byte) (err error) {
    var n int
    for i := 0; i < len(buf); i += n {
        n, err = a.conn.Write(buf[i:])
        if err != nil {
            return err
        }
    }
    return
}
