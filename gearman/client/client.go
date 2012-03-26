// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

import (
    "bitbucket.org/mikespook/gearman-go/gearman"
    "bytes"
    "io"
    "net"
    "strconv"
    "sync"
)

/* 
The client side api for gearman.

usage:
    client = NewClient()
    client.AddServer("127.0.0.1:4730")
    handle := client.Do("foobar", []byte("data here"), JOB_LOW | JOB_BG)

*/
type Client struct {
    mutex    sync.Mutex
    conn     net.Conn
    JobQueue chan *ClientJob
    incoming chan []byte
    UId      uint32
}

// Create a new client.
func NewClient() (client *Client) {
    client = &Client{JobQueue: make(chan *ClientJob, gearman.QUEUE_CAP),
        incoming: make(chan []byte, gearman.QUEUE_CAP),
        UId:      1}
    return
}

// Add a server.
// In this version, one client connect to one job server.
// Sample is better. Plz do the load balancing by your self.
func (client *Client) AddServer(addr string) (err error) {
    conn, err := net.Dial(gearman.TCP, addr)
    if err != nil {
        return
    }
    client.conn = conn
    return
}

// Internal read
func (client *Client) read() (data []byte, err error) {
    if len(client.incoming) > 0 {
        // incoming queue is not empty
        data = <-client.incoming
    } else {
        // empty queue, read data from socket
        for {
            buf := make([]byte, gearman.BUFFER_SIZE)
            var n int
            if n, err = client.conn.Read(buf); err != nil {
                if err == io.EOF && n == 0 {
                    break
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
    start, end := 0, 4
    tl := len(data)
    for i := 0; i < tl; i++ {
        if string(data[start:end]) == gearman.RES_STR {
            l := int(gearman.BytesToUint32([4]byte{data[start+8], data[start+9], data[start+10], data[start+11]}))
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
            end++
        }
    }
    err = gearman.ErrInvalidData
    return
}

// Read a job from job server.
// This function will return the job, and add it to the job queue.
func (client *Client) ReadJob() (job *ClientJob, err error) {
    var rel []byte
    if rel, err = client.read(); err != nil {
        return
    }
    if job, err = DecodeClientJob(rel); err != nil {
        return
    } else {
        switch job.DataType {
        case gearman.ERROR:
            _, err = gearman.GetError(job.Data)
            return
        case gearman.WORK_DATA, gearman.WORK_WARNING, gearman.WORK_STATUS, gearman.WORK_COMPLETE, gearman.WORK_FAIL, gearman.WORK_EXCEPTION:
            client.JobQueue <- job
        }
    }
    return
}

// Do the function.
// funcname is a string with function name.
// data is encoding to byte array.
// flag set the job type, include running level: JOB_LOW, JOB_NORMAL, JOB_HIGH,
// and if it is background job: JOB_BG.
// JOB_LOW | JOB_BG means the job is running with low level in background.
func (client *Client) Do(funcname string, data []byte, flag byte) (handle string, err error) {
    var datatype uint32
    if flag&gearman.JOB_LOW == gearman.JOB_LOW {
        if flag&gearman.JOB_BG == gearman.JOB_BG {
            datatype = gearman.SUBMIT_JOB_LOW_BG
        } else {
            datatype = gearman.SUBMIT_JOB_LOW
        }
    } else if flag&gearman.JOB_HIGH == gearman.JOB_HIGH {
        if flag&gearman.JOB_BG == gearman.JOB_BG {
            datatype = gearman.SUBMIT_JOB_HIGH_BG
        } else {
            datatype = gearman.SUBMIT_JOB_HIGH
        }
    } else if flag&gearman.JOB_BG == gearman.JOB_BG {
        datatype = gearman.SUBMIT_JOB_BG
    } else {
        datatype = gearman.SUBMIT_JOB
    }

    rel := make([]byte, 0, 1024*64)
    rel = append(rel, []byte(funcname)...)
    rel = append(rel, '\x00')
    client.mutex.Lock()
    uid := strconv.Itoa(int(client.UId))
    client.UId++
    rel = append(rel, []byte(uid)...)
    client.mutex.Unlock()
    rel = append(rel, '\x00')
    rel = append(rel, data...)
    if err = client.WriteJob(NewClientJob(gearman.REQ, datatype, rel)); err != nil {
        return
    }
    var job *ClientJob
    if job, err = client.readLastJob(gearman.JOB_CREATED); err != nil {
        return
    }
    handle = string(job.Data)
    go func() {
        if flag&gearman.JOB_BG != gearman.JOB_BG {
            for {
                if job, err = client.ReadJob(); err != nil {
                    return
                }
                switch job.DataType {
                case gearman.WORK_DATA, gearman.WORK_WARNING:
                case gearman.WORK_STATUS:
                case gearman.WORK_COMPLETE, gearman.WORK_FAIL, gearman.WORK_EXCEPTION:
                    return
                }
            }
        }
    }()
    return
}

// Internal read last job
func (client *Client) readLastJob(datatype uint32) (job *ClientJob, err error) {
    for {
        if job, err = client.ReadJob(); err != nil {
            return
        }
        if job.DataType == datatype {
            break
        }
    }
    if job.DataType != datatype {
        err = gearman.ErrDataType
    }
    return
}

// Get job status from job server.
// !!!Not fully tested.!!!
func (client *Client) Status(handle string) (known, running bool, numerator, denominator uint64, err error) {

    if err = client.WriteJob(NewClientJob(gearman.REQ, gearman.GET_STATUS, []byte(handle))); err != nil {
        return
    }
    var job *ClientJob
    if job, err = client.readLastJob(gearman.STATUS_RES); err != nil {
        return
    }
    data := bytes.SplitN(job.Data, []byte{'\x00'}, 5)
    if len(data) != 5 {
        err = gearman.ErrInvalidData
        return
    }
    if handle != string(data[0]) {
        err = gearman.ErrInvalidData
        return
    }
    known = data[1][0] == '1'
    running = data[2][0] == '1'
    if numerator, err = strconv.ParseUint(string(data[3][0]), 10, 0); err != nil {
        return
    }
    if denominator, err = strconv.ParseUint(string(data[4][0]), 10, 0); err != nil {
        return
    }
    return
}

// Send a something out, get the samething back.
func (client *Client) Echo(data []byte) (echo []byte, err error) {
    if err = client.WriteJob(NewClientJob(gearman.REQ, gearman.ECHO_REQ, data)); err != nil {
        return
    }
    var job *ClientJob
    if job, err = client.readLastJob(gearman.ECHO_RES); err != nil {
        return
    }
    echo = job.Data
    return
}

// Get the last job.
// the job means a network package. 
// Normally, it is the job executed result.
func (client *Client) LastJob() (job *ClientJob) {
    if l := len(client.JobQueue); l != 1 {
        if l == 0 {
            return
        }
        for i := 0; i < l-1; i++ {
            <-client.JobQueue
        }
    }
    return <-client.JobQueue
}

// Send the job to job server.
func (client *Client) WriteJob(job *ClientJob) (err error) {
    return client.write(job.Encode())
}

// Internal write
func (client *Client) write(buf []byte) (err error) {
    var n int
    for i := 0; i < len(buf); i += n {
        n, err = client.conn.Write(buf[i:])
        if err != nil {
            return
        }
    }
    return
}

// Close.
func (client *Client) Close() (err error) {
    err = client.conn.Close()
    close(client.JobQueue)
    return
}
