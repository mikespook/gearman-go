// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
    "github.com/miraclesu/gearman-go/common"
    "io"
    "net"
)

// The agent of job server.
type agent struct {
    conn   net.Conn
    worker *Worker
    in     chan []byte
    out    chan *Job
    addr   string
}

// Create the agent of job server.
func newAgent(addr string, worker *Worker) (a *agent, err error) {
    conn, err := net.Dial(common.NETWORK, addr)
    if err != nil {
        return
    }
    a = &agent{
        conn:   conn,
        worker: worker,
        addr:   addr,
        in:     make(chan []byte, common.QUEUE_SIZE),
        out:    make(chan *Job, common.QUEUE_SIZE),
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
                for i := 0; i < 3 && a.worker.running; i++ {
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

func (a *agent) readData(length int) (data []byte, err error) {
    n := 0
    buf := make([]byte, common.BUFFER_SIZE)
    // read until data can be unpacked
    for i := length; i > 0 || len(data) < common.PACKET_LEN; i -= n {
        if n, err = a.conn.Read(buf); err != nil {
            if err == io.EOF && n == 0 {
                if data == nil {
                    err = common.ErrConnection
                    return
                }
                return data, nil
            }
            return
        }
        data = append(data, buf[0:n]...)
        if n < common.BUFFER_SIZE {
            break
        }
    }
    return
}

func (a *agent) unpack(data []byte) ([]byte, int, bool) {
    tl := len(data)
    start := 0
    for i := 0; i < tl+1-common.PACKET_LEN; i++ {
        if start+common.PACKET_LEN > tl { // too few data to unpack, read more
            return nil, common.PACKET_LEN, false
        }
        if string(data[start:start+4]) == common.RES_STR {
            l := int(common.BytesToUint32([4]byte{data[start+8],
                data[start+9], data[start+10], data[start+11]}))
            total := l + common.PACKET_LEN
            if total == tl { // data is what we want
                return data, common.PACKET_LEN, true
            } else if total < tl { // data[:total] is what we want, data[total:] is the more
                a.in <- data[total:]
                data = data[start:total]
                return data, common.PACKET_LEN, true
            } else { // ops! It won't be possible.
                return nil, total - tl, false
            }
        } else { // flag was not found, move to next step
            start++
        }
    }
    return nil, common.PACKET_LEN, false
}

func (a *agent) read() (rel []byte, err error) {
    var data []byte
    ok := false
    l := common.PACKET_LEN
    for !ok {
        inlen := len(a.in)
        if inlen > 0 {
            // in queue is not empty
            for i := 0; i < inlen; i++ {
                data = append(data, <-a.in...)
            }
        } else {
            var d []byte
            d, err = a.readData(l)
            if err != nil {
                return
            }
            data = append(data, d...)
        }
        rel, l, ok = a.unpack(data)
    }
    return
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
