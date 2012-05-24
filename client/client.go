// Copyright 2011 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

import (
    "io"
    "net"
    "time"
    "bytes"
    "strconv"
    "bitbucket.org/mikespook/golib/autoinc"
    "bitbucket.org/mikespook/gearman-go/common"
)

// Job handler
type JobHandler func(*Job) error
// Status handler
// handle, known, running, numerator, denominator
type StatusHandler func(string, bool, bool, uint64, uint64)

/* 
The client side api for gearman

usage:
c := client.New("tcp4", "127.0.0.1:4730")
handle := c.Do("foobar", []byte("data here"), JOB_LOW | JOB_BG)

*/
type Client struct {
    ErrHandler common.ErrorHandler
    JobHandler JobHandler
    StatusHandler StatusHandler
    TimeOut time.Duration

    in chan []byte
    out chan *Job
    jobCreated chan *Job
    conn     net.Conn
    ai *autoinc.AutoInc
}

// Create a new client.
// Connect to "addr" through "network"
// Eg.
//      client, err := client.New("127.0.0.1:4730")
func New(addr string) (client *Client, err error) {
    conn, err := net.Dial(common.NETWORK, addr)
    if err != nil {
        return
    }
    client = &Client{
        jobCreated: make(chan *Job),
        in: make(chan []byte, common.QUEUE_SIZE),
        out: make(chan *Job, common.QUEUE_SIZE),
        conn: conn,
        ai: autoinc.New(0, 1),
        TimeOut: time.Second,
    }
    go client.inLoop()
    go client.outLoop()
    return
}

// out loop
func (client *Client) outLoop() {
    ok := true
    for ok {
        if job, ok := <-client.out; ok {
            if err := client.write(job.Encode()); err != nil {
                client.err(err)
            }
        }
    }
}

// in loop
func (client *Client) inLoop() {
    defer common.DisablePanic()
    for {
        rel, err := client.read()
        if err != nil {
            if err == common.ErrConnection {
                client.Close()
                break
            }
            client.err(err)
            continue
        }
        job, err := decodeJob(rel)
        if err != nil {
            client.err(err)
            continue
        }
        switch job.DataType {
        case common.ERROR:
            _, err := common.GetError(job.Data)
            client.err(err)
            case common.WORK_DATA, common.WORK_WARNING, common.WORK_STATUS,
            common.WORK_COMPLETE, common.WORK_FAIL, common.WORK_EXCEPTION,
            common.ECHO_RES:
            go client.handleJob(job)
        case common.JOB_CREATED:
            client.jobCreated <- job
        case common.STATUS_RES:
            go client.handleStatus(job)
        }
    }
}

// inner read
func (client *Client) read() (data []byte, err error) {
    if len(client.in) > 0 {
        // incoming queue is not empty
        data = <-client.in
    } else {
        // empty queue, read data from socket
        for {
            buf := make([]byte, common.BUFFER_SIZE)
            var n int
            if n, err = client.conn.Read(buf); err != nil {
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
    start, end := 0, 4
    for i := 0; i < tl; i++ {
        if string(data[start:end]) == common.RES_STR {
            l := int(common.BytesToUint32([4]byte{data[start+8], data[start+9], data[start+10], data[start+11]}))
            total := l + 12
            if total == tl {
                return
            } else {
                client.in <- data[total:]
                data = data[:total]
                return
            }
        } else {
            start++
            end++
        }
    }
    return nil, common.Errorf("Invalid data: %V", data)
}

// error handler
func (client *Client) err (e error) {
    if client.ErrHandler != nil {
        client.ErrHandler(e)
    }
}

// job handler
func (client *Client) handleJob(job *Job) {
    if client.JobHandler != nil {
        if err := client.JobHandler(job); err != nil {
            client.err(err)
        }
    }
}

// status handler
func (client *Client) handleStatus(job *Job) {
    if client.StatusHandler != nil {
        data := bytes.SplitN(job.Data, []byte{'\x00'}, 5)
        if len(data) != 5 {
            client.err(common.Errorf("Invalid data: %V", job.Data))
            return
        }
        handle := string(data[0])
        known := (data[1][0] == '1')
        running := (data[2][0] == '1')
        numerator, err := strconv.ParseUint(string(data[3][0]), 10, 0)
        if err != nil {
            client.err(common.Errorf("Invalid handle: %s", data[3][0]))
            return
        }
        denominator, err := strconv.ParseUint(string(data[4][0]), 10, 0)
        if err != nil {
            client.err(common.Errorf("Invalid handle: %s", data[4][0]))
            return
        }
        client.StatusHandler(handle, known, running, numerator, denominator)
    }
}

// Do the function.
// funcname is a string with function name.
// data is encoding to byte array.
// flag set the job type, include running level: JOB_LOW, JOB_NORMAL, JOB_HIGH,
// and if it is background job: JOB_BG.
// JOB_LOW | JOB_BG means the job is running with low level in background.
func (client *Client) Do(funcname string, data []byte, flag byte) (handle string, err error) {
    var datatype uint32
    if flag & JOB_LOW == JOB_LOW {
        if flag & JOB_BG == JOB_BG {
            datatype = common.SUBMIT_JOB_LOW_BG
        } else {
            datatype = common.SUBMIT_JOB_LOW
        }
    } else if flag & JOB_HIGH == JOB_HIGH {
        if flag & JOB_BG == JOB_BG {
            datatype = common.SUBMIT_JOB_HIGH_BG
        } else {
            datatype = common.SUBMIT_JOB_HIGH
        }
    } else if flag & JOB_BG == JOB_BG {
        datatype = common.SUBMIT_JOB_BG
    } else {
        datatype = common.SUBMIT_JOB
    }

    uid := strconv.Itoa(int(client.ai.Id()))
    l := len(funcname) + len(uid) + len(data) + 2
    rel := make([]byte, 0, l)
    rel = append(rel, []byte(funcname)...)          // len(funcname)
    rel = append(rel, '\x00')                       // 1 Byte
    rel = append(rel, []byte(uid)...)               // len(uid)
    rel = append(rel, '\x00')                       // 1 Byte
    rel = append(rel, data...)                      // len(data)
    client.writeJob(newJob(common.REQ, datatype, rel))
    // Waiting for JOB_CREATED
    timeout := make(chan bool)
    defer close(timeout)
    go func() {
        defer common.DisablePanic()
        time.Sleep(client.TimeOut)
        timeout <- true
    }()
    select {
    case job := <-client.jobCreated:
        return string(job.Data), nil
    case <-timeout:
        return "", common.ErrJobTimeOut
    }
    return
}

// Get job status from job server.
// !!!Not fully tested.!!!
func (client *Client) Status(handle string) {
    job := newJob(common.REQ, common.GET_STATUS, []byte(handle))
    client.writeJob(job)
}

// Send a something out, get the samething back.
func (client *Client) Echo(data []byte) {
    client.writeJob(newJob(common.REQ, common.ECHO_REQ, data))
}

// Send the job to job server.
func (client *Client) writeJob(job *Job) {
    client.out <- job
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

// Close
func (client *Client) Close() (err error) {
    close(client.jobCreated)
    close(client.in)
    close(client.out)
    return client.conn.Close();
}
