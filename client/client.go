// Copyright 2011 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

import (
    "io"
    "net"
    "sync"
    "time"
    "bytes"
    "strconv"
    "github.com/mikespook/golib/autoinc"
    "github.com/mikespook/gearman-go/common"
)

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
    TimeOut time.Duration

    in chan []byte
    out chan *Job

    created chan string
    echo chan []byte
    status chan *Status

    jobhandlers map[string]JobHandler

    conn net.Conn
    addr string
    ai *autoinc.AutoInc
    mutex sync.RWMutex
}

// Create a new client.
// Connect to "addr" through "network"
// Eg.
//      client, err := client.New("127.0.0.1:4730")
func New(addr string) (client *Client, err error) {
    client = &Client{
        created: make(chan string, common.QUEUE_SIZE),
        echo: make(chan []byte, common.QUEUE_SIZE),
        status: make(chan *Status, common.QUEUE_SIZE),

        jobhandlers: make(map[string]JobHandler, common.QUEUE_SIZE),

        in: make(chan []byte, common.QUEUE_SIZE),
        out: make(chan *Job, common.QUEUE_SIZE),
        addr: addr,
        ai: autoinc.New(0, 1),
        TimeOut: time.Second,
    }
    if err = client.connect(); err != nil {
        return
    }
    go client.inLoop()
    go client.outLoop()
    return
}

// {{{ private functions

// 
func (client *Client) connect() (err error) {
    client.mutex.Lock()
    defer client.mutex.Unlock()
    client.conn, err = net.Dial(common.NETWORK, client.addr)
    return
}

// Internal write
func (client *Client) write(buf []byte) (err error) {
    client.mutex.RLock()
    defer client.mutex.RUnlock()
    var n int
    for i := 0; i < len(buf); i += n {
        n, err = client.conn.Write(buf[i:])
        if err != nil {
            return
        }
    }
    return
}

// read length bytes from the socket
func (client *Client) readData(length int) (data []byte, err error) {
    client.mutex.RLock()
    defer client.mutex.RUnlock()
    n := 0
    buf := make([]byte, common.BUFFER_SIZE)
    // read until data can be unpacked
    for i := length; i > 0 || len(data) < common.PACKET_LEN; i -= n {
        if n, err = client.conn.Read(buf); err != nil {
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

// unpack data
func (client *Client) unpack(data []byte) ([]byte, int, bool) {
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
                client.in <- data[total:]
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

// Internal read
func (client *Client) read() (rel []byte, err error) {
    var data []byte
    ok := false
    l := common.PACKET_LEN
    for !ok {
        inlen := len(client.in)
        if inlen > 0 {
            // in queue is not empty
            for i := 0; i < inlen; i++ {
                data = append(data, <-client.in...)
            }
        } else {
            var d []byte
            d, err = client.readData(l)
            if err != nil {
                return
            }
            data = append(data, d...)
        }
        rel, l, ok = client.unpack(data)
    }
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
            common.WORK_COMPLETE, common.WORK_FAIL, common.WORK_EXCEPTION:
            client.handleJob(job)
        case common.ECHO_RES:
            client.handleEcho(job)
        case common.JOB_CREATED:
            client.handleCreated(job)
        case common.STATUS_RES:
            client.handleStatus(job)
        }
    }
}

// error handler
func (client *Client) err (e error) {
    if client.ErrHandler != nil {
        client.ErrHandler(e)
    }
}

// job handler
func (client *Client) handleJob(job *Job) {
    if h, ok := client.jobhandlers[job.UniqueId]; ok {
        h(job)
        delete(client.jobhandlers, job.UniqueId)
    }
}

func (client *Client) handleEcho(job *Job) {
    client.echo <- job.Data
}

func (client *Client) handleCreated(job *Job) {
    client.created <- string(job.Data)
}

// status handler
func (client *Client) handleStatus(job *Job) {
    data := bytes.SplitN(job.Data, []byte{'\x00'}, 5)
    if len(data) != 5 {
        client.err(common.Errorf("Invalid data: %V", job.Data))
        return
    }
    status := &Status{}
    status.Handle = string(data[0])
    status.Known = (data[1][0] == '1')
    status.Running = (data[2][0] == '1')
    var err error
    status.Numerator, err = strconv.ParseUint(string(data[3][0]), 10, 0)
    if err != nil {
        client.err(common.Errorf("Invalid handle: %s", data[3][0]))
        return
    }
    status.Denominator, err = strconv.ParseUint(string(data[4][0]), 10, 0)
    if err != nil {
        client.err(common.Errorf("Invalid handle: %s", data[4][0]))
        return
    }
    client.status <- status
}

// Send the job to job server.
func (client *Client) writeJob(job *Job) {
    client.out <- job
}

// Internal do
func (client *Client) do(funcname string, data []byte,
flag uint32) (id string, handle string) {
    id = strconv.Itoa(int(client.ai.Id()))
    l := len(funcname) + len(id) + len(data) + 2
    rel := make([]byte, 0, l)
    rel = append(rel, []byte(funcname)...)          // len(funcname)
    rel = append(rel, '\x00')                       // 1 Byte
    rel = append(rel, []byte(id)...)               // len(uid)
    rel = append(rel, '\x00')                       // 1 Byte
    rel = append(rel, data...)                      // len(data)
    client.writeJob(newJob(common.REQ, flag, rel))
    // Waiting for JOB_CREATED
    handle = <-client.created
    return
}

// }}}

// Do the function.
// funcname is a string with function name.
// data is encoding to byte array.
// flag set the job type, include running level: JOB_LOW, JOB_NORMAL, JOB_HIGH,
// and if it is background job: JOB_BG.
// JOB_LOW | JOB_BG means the job is running with low level in background.
func (client *Client) Do(funcname string, data []byte,
flag byte, jobhandler JobHandler) (handle string) {
    var datatype uint32
    switch flag {
    case JOB_LOW :
        datatype = common.SUBMIT_JOB_LOW
    case JOB_HIGH :
        datatype = common.SUBMIT_JOB_HIGH
    default:
        datatype = common.SUBMIT_JOB
    }
    var id string
    id, handle = client.do(funcname, data, datatype)
    if jobhandler != nil {
        client.jobhandlers[id] = jobhandler
    }
    return
}

func (client *Client) DoBg(funcname string, data []byte,
flag byte) (handle string) {
    var datatype uint32
    switch flag {
    case JOB_LOW :
        datatype = common.SUBMIT_JOB_LOW_BG
    case JOB_HIGH :
        datatype = common.SUBMIT_JOB_HIGH_BG
    default:
        datatype = common.SUBMIT_JOB_BG
    }
    _, handle = client.do(funcname, data, datatype)
    return
}


// Get job status from job server.
// !!!Not fully tested.!!!
func (client *Client) Status(handle string) (status *Status) {
    client.writeJob(newJob(common.REQ, common.GET_STATUS, []byte(handle)))
    status = <-client.status
    return
}

// Send a something out, get the samething back.
func (client *Client) Echo(data []byte) (r []byte) {
    client.writeJob(newJob(common.REQ, common.ECHO_REQ, data))
    r = <-client.echo
    return
}

// Close
func (client *Client) Close() (err error) {
    close(client.in)
    close(client.out)

    close(client.echo)
    close(client.created)
    close(client.status)

    return client.conn.Close();
}
