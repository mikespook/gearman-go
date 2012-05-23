// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
    "io"
    "net"
    "bitbucket.org/mikespook/gearman-go/common"
)

// The agent of job server.
type jobAgent struct {
    conn     net.Conn
    worker   *Worker
    running  bool
    in chan []byte
    out chan *Job
}

// Create the agent of job server.
func newJobAgent(addr string, worker *Worker) (jobagent *jobAgent, err error) {
    conn, err := net.Dial(common.NETWORK, addr)
    if err != nil {
        return nil, err
    }
    jobagent = &jobAgent{
        conn: conn,
        worker: worker,
        running: true,
        in: make(chan []byte, common.QUEUE_SIZE),
    }
    return jobagent, err
}

// Internal read
func (agent *jobAgent) read() (data []byte, err error) {
    if len(agent.in) > 0 {
        // in queue is not empty
        data = <-agent.in
    } else {
        for {
            buf := make([]byte, common.BUFFER_SIZE)
            var n int
            if n, err = agent.conn.Read(buf); err != nil {
                if err == io.EOF && n == 0 {
                    err = nil
                    return
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
    start := 0
    tl := len(data)
    for i := 0; i < tl; i++ {
        if string(data[start:start+4]) == common.RES_STR {
            l := int(common.BytesToUint32([4]byte{data[start+8], data[start+9], data[start+10], data[start+11]}))
            total := l + 12
            if total == tl {
                return
            } else {
                agent.in <- data[total:]
                data = data[:total]
                return
            }
        } else {
            start++
        }
    }
    err = common.ErrInvalidData
    return
}

// Main loop.
func (agent *jobAgent) Work() {
    noop := true
    for agent.running {
        // got noop msg and in queue is zero, grab job
        if noop && len(agent.in) == 0 {
            agent.WriteJob(newJob(common.REQ, common.GRAB_JOB, nil))
        }
        rel, err := agent.read()
        if err != nil {
            agent.worker.err(err)
            continue
        }
        job, err := decodeJob(rel)
        if err != nil {
            agent.worker.err(err)
            continue
        } else {
            switch job.DataType {
            case common.NOOP:
                noop = true
            case common.NO_JOB:
                noop = false
                agent.WriteJob(newJob(common.REQ, common.PRE_SLEEP, nil))
            case common.ECHO_RES, common.JOB_ASSIGN_UNIQ, common.JOB_ASSIGN:
                job.agent = agent
                agent.worker.in <- job
            }
        }
    }
    return
}

// Send a job to the job server.
func (agent *jobAgent) WriteJob(job *Job) (err error) {
    return agent.write(job.Encode())
}

// Internal write the encoded job.
func (agent *jobAgent) write(buf []byte) (err error) {
    var n int
    for i := 0; i < len(buf); i += n {
        n, err = agent.conn.Write(buf[i:])
        if err != nil {
            return err
        }
    }
    return
}

// Close.
func (agent *jobAgent) Close() (err error) {
    agent.running = false
    close(agent.in)
    err = agent.conn.Close()
    return
}
