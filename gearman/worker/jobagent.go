// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
    "bitbucket.org/mikespook/gearman-go/gearman"
    "io"
    "net"
)

// The agent of job server.
type jobAgent struct {
    conn     net.Conn
    worker   *Worker
    running  bool
    incoming chan []byte
}

// Create the agent of job server.
func newJobAgent(addr string, worker *Worker) (jobagent *jobAgent, err error) {
    conn, err := net.Dial(gearman.TCP, addr)
    if err != nil {
        return nil, err
    }
    jobagent = &jobAgent{conn: conn, worker: worker, running: true, incoming: make(chan []byte, gearman.QUEUE_CAP)}
    return jobagent, err
}

// Internal read
func (agent *jobAgent) read() (data []byte, err error) {
    if len(agent.incoming) > 0 {
        // incoming queue is not empty
        data = <-agent.incoming
    } else {
        for {
            buf := make([]byte, gearman.BUFFER_SIZE)
            var n int
            if n, err = agent.conn.Read(buf); err != nil {
                if err == io.EOF && n == 0 {
                    err = nil
                    return
                }
                return
            }
            data = append(data, buf[0:n]...)
            if n < gearman.BUFFER_SIZE {
                break
            }
        }
    }
    // split package
    start := 0
    tl := len(data)
    for i := 0; i < tl; i++ {
        if string(data[start:start+4]) == gearman.RES_STR {
            l := int(gearman.BytesToUint32([4]byte{data[start+8], data[start+9], data[start+10], data[start+11]}))
            total := l + 12
            if total == tl {
                return
            } else {
                agent.incoming <- data[total:]
                data = data[:total]
                return
            }
        } else {
            start++
        }
    }
    err = gearman.ErrInvalidData
    return
}

// Main loop.
func (agent *jobAgent) Work() {
    noop := true
    for agent.running {
        // got noop msg and incoming queue is zero, grab job
        if noop && len(agent.incoming) == 0 {
            agent.WriteJob(NewWorkerJob(gearman.REQ, gearman.GRAB_JOB, nil))
        }
        rel, err := agent.read()
        if err != nil {
            agent.worker.ErrQueue <- err
            continue
        }
        job, err := DecodeWorkerJob(rel)
        if err != nil {
            agent.worker.ErrQueue <- err
            continue
        } else {
            switch job.DataType {
            case gearman.NOOP:
                noop = true
            case gearman.NO_JOB:
                noop = false
                agent.WriteJob(NewWorkerJob(gearman.REQ, gearman.PRE_SLEEP, nil))
            case gearman.ECHO_RES, gearman.JOB_ASSIGN_UNIQ, gearman.JOB_ASSIGN:
                job.agent = agent
                agent.worker.incoming <- job
            }
        }
    }
    return
}

// Send a job to the job server.
func (agent *jobAgent) WriteJob(job *WorkerJob) (err error) {
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
    close(agent.incoming)
    err = agent.conn.Close()
    return
}
