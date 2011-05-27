// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package gearman

import (
    "net"
    "os"
    //    "log"
)

// The client of job server.
type jobClient struct {
    conn     net.Conn
    worker   *Worker
    running  bool
    incoming chan []byte
}

// Create the client of job server.
func newJobClient(addr string, worker *Worker) (jobclient *jobClient, err os.Error) {
    conn, err := net.Dial(TCP, addr)
    if err != nil {
        return nil, err
    }
    jobclient = &jobClient{conn: conn, worker: worker, running: true, incoming: make(chan []byte, QUEUE_CAP)}
    return jobclient, err
}

// Internal read
func (client *jobClient) read() (data []byte, err os.Error) {
    if len(client.incoming) > 0 {
        // incoming queue is not empty
        data = <-client.incoming
    } else {
        for {
            buf := make([]byte, BUFFER_SIZE)
            var n int
            if n, err = client.conn.Read(buf); err != nil {
                if err == os.EOF && n == 0 {
                    err = nil
                    return
                }
                return
            }
            data = append(data, buf[0:n]...)
            if n < BUFFER_SIZE {
                break
            }
        }
    }
    // split package
    start := 0
    tl := len(data)
    for i := 0; i < tl; i++ {
        if string(data[start:start+4]) == RES_STR {
            l := int(byteToUint32([4]byte{data[start+8], data[start+9], data[start+10], data[start+11]}))
            total := l + 12
            if total == tl {
                return
            } else {
                client.incoming <- data[total:]
                data = data[:total]
                return
            }
        } else {
            start++
        }
    }
    err = os.NewError("Invalid data struct.")
    return
}

// Main loop.
func (client *jobClient) Work() {
    noop := true
    for client.running {
        // got noop msg and incoming queue is zero, grab job
        if noop && len(client.incoming) == 0 {
            client.WriteJob(NewWorkerJob(REQ, GRAB_JOB, nil))
        }
        rel, err := client.read()
        if err != nil {
            client.worker.ErrQueue <- err
            continue
        }
        job, err := DecodeWorkerJob(rel)
        if err != nil {
            client.worker.ErrQueue <- err
            continue
        } else {
            switch job.DataType {
            case NOOP:
                noop = true
            case NO_JOB:
                noop = false
                client.WriteJob(NewWorkerJob(REQ, PRE_SLEEP, nil))
            case ECHO_RES, JOB_ASSIGN_UNIQ, JOB_ASSIGN:
                job.client = client
                client.worker.incoming <- job
            }
        }
    }
    return
}

// Send a job to the job server.
func (client *jobClient) WriteJob(job *WorkerJob) (err os.Error) {
    return client.write(job.Encode())
}

// Internal write the encoded job.
func (client *jobClient) write(buf []byte) (err os.Error) {
    var n int
    for i := 0; i < len(buf); i += n {
        n, err = client.conn.Write(buf[i:])
        if err != nil {
            return err
        }
    }
    return
}

// Close.
func (client *jobClient) Close() (err os.Error) {
    client.running = false
    close(client.incoming)
    err = client.conn.Close()
    return
}
