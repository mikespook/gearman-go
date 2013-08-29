// Copyright 2011 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

import (
	"github.com/mikespook/golib/idgen"
	"io"
	"net"
	"sync"
)

/*
The client side api for gearman

usage:
c := client.New("tcp4", "127.0.0.1:4730")
handle := c.Do("foobar", []byte("data here"), JOB_LOW | JOB_BG)

*/
type Client struct {
	net, addr      string
	respHandler    map[string]ResponseHandler
	innerHandler   map[string]ResponseHandler
	in             chan []byte
	isConn         bool
	conn           net.Conn
	mutex          sync.RWMutex
	ErrorHandler   ErrorHandler

	IdGen idgen.IdGen
}

// Create a new client.
// Connect to "addr" through "network"
// Eg.
//      client, err := client.New("127.0.0.1:4730")
func New(net, addr string) (client *Client, err error) {
	client = &Client{
		net:         net,
		addr:        addr,
		respHandler: make(map[string]ResponseHandler, QUEUE_SIZE),
		innerHandler: make(map[string]ResponseHandler, QUEUE_SIZE),
		in:          make(chan []byte, QUEUE_SIZE),
		IdGen:       idgen.NewObjectId(),
	}
	if err = client.connect(); err != nil {
		return
	}
	client.isConn = true
	go client.readLoop()
	go client.processLoop()
	return
}

// {{{ private functions

//
func (client *Client) connect() (err error) {
	client.conn, err = net.Dial(client.net, client.addr)
	return
}

// Internal write
func (client *Client) write(req *request) (err error) {
	var n int
	buf := req.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = client.conn.Write(buf[i:])
		if err != nil {
			return
		}
	}
	return
}

// read length bytes from the socket
func (client *Client) read(length int) (data []byte, err error) {
	n := 0
	buf := getBuffer(BUFFER_SIZE)
	// read until data can be unpacked
	for i := length; i > 0 || len(data) < MIN_PACKET_LEN; i -= n {
		if n, err = client.conn.Read(buf); err != nil {
			if !client.isConn {
				err = ErrConnClosed
				return
			}
			if err == io.EOF && n == 0 {
				if data == nil {
					err = ErrConnection
				}
			}
			return
		}
		data = append(data, buf[0:n]...)
		if n < BUFFER_SIZE {
			break
		}
	}
	return
}

// read data from socket
func (client *Client) readLoop() {
	var data []byte
	var err error
	for client.isConn {
		if data, err = client.read(BUFFER_SIZE); err != nil {
			if err == ErrConnClosed {
				break
			}
			client.err(err)
			continue
		}
		client.in <- data
	}
	close(client.in)
}

// decode data & process it
func (client *Client) processLoop() {
	var resp *Response
	var l int
	var err error
	var data, leftdata []byte
	for data = range client.in {
		l = len(data)
		if len(leftdata) > 0 { // some data left for processing
			data = append(leftdata, data...)
		}
		if l < MIN_PACKET_LEN { // not enough data
			leftdata = data
			continue
		}
		if resp, l, err = decodeResponse(data); err != nil {
			client.err(err)
			continue
		}
		switch resp.DataType {
		case STATUS_RES:
			client.handleInner("status-" + resp.Handle, resp)
		case JOB_CREATED:
			client.handleInner("created", resp)
		case ECHO_RES:
			client.handleInner("echo", resp)
		case WORK_DATA, WORK_WARNING, WORK_STATUS, WORK_COMPLETE,
			WORK_FAIL, WORK_EXCEPTION:
			client.handleResponse(resp.Handle, resp)
		}
		if len(data) > l {
			leftdata = data[l:]
		}
	}
}

// error handler
func (client *Client) err(e error) {
	if client.ErrorHandler != nil {
		client.ErrorHandler(e)
	}
}

// job handler
func (client *Client) handleResponse(key string, resp *Response) {
	client.mutex.RLock()
	defer client.mutex.RUnlock()
	if h, ok := client.respHandler[key]; ok {
		h(resp)
		delete(client.respHandler, key)
	}
}

// job handler
func (client *Client) handleInner(key string, resp *Response) {
	if h, ok := client.innerHandler[key]; ok {
		h(resp)
		delete(client.innerHandler, key)
	}
}

// Internal do
func (client *Client) do(funcname string, data []byte,
	flag uint32) (handle string) {
	id := client.IdGen.Id().(string)
	req := getJob(funcname, id, data)
	client.write(req)
	var wg sync.WaitGroup
	wg.Add(1)
	client.mutex.RLock()
	client.innerHandler["created"] = ResponseHandler(func(resp *Response) {
		defer wg.Done()
		defer client.mutex.RUnlock()
		handle = resp.Handle
	})
	wg.Wait()
	return
}

// }}}

// Do the function.
// funcname is a string with function name.
// data is encoding to byte array.
// flag set the job type, include running level: JOB_LOW, JOB_NORMAL, JOB_HIGH
func (client *Client) Do(funcname string, data []byte,
	flag byte, h ResponseHandler) (handle string) {
	var datatype uint32
	switch flag {
	case JOB_LOW:
		datatype = SUBMIT_JOB_LOW
	case JOB_HIGH:
		datatype = SUBMIT_JOB_HIGH
	default:
		datatype = SUBMIT_JOB
	}
	handle = client.do(funcname, data, datatype)
	client.mutex.Lock()
	defer client.mutex.Unlock()
	if h != nil {
		client.respHandler[handle] = h
	}
	return
}

// Do the function at background.
// funcname is a string with function name.
// data is encoding to byte array.
// flag set the job type, include running level: JOB_LOW, JOB_NORMAL, JOB_HIGH
func (client *Client) DoBg(funcname string, data []byte,
	flag byte) (handle string) {
	var datatype uint32
	switch flag {
	case JOB_LOW:
		datatype = SUBMIT_JOB_LOW_BG
	case JOB_HIGH:
		datatype = SUBMIT_JOB_HIGH_BG
	default:
		datatype = SUBMIT_JOB_BG
	}
	handle = client.do(funcname, data, datatype)
	return
}

// Get job status from job server.
// !!!Not fully tested.!!!
func (client *Client) Status(handle string, h ResponseHandler) (status *Status, err error) {
	req := getRequest()
	req.DataType = GET_STATUS
	req.Data = []byte(handle)
	client.write(req)
	var wg sync.WaitGroup
	wg.Add(1)
	client.mutex.Lock()
	client.innerHandler["status-" + handle] = ResponseHandler(func(resp *Response) {
		defer wg.Done()
		defer client.mutex.Unlock()
		var err error
		status, err = resp.Status()
		if err != nil {
			client.err(err)
		}
	})
	wg.Wait()
	return
}

// Send a something out, get the samething back.
func (client *Client) Echo(data []byte) (echo []byte, err error) {
	req := getRequest()
	req.DataType = ECHO_REQ
	req.Data = data
	client.write(req)
	var wg sync.WaitGroup
	wg.Add(1)
	client.mutex.Lock()
	client.innerHandler["echo"] = ResponseHandler(func(resp *Response) {
		defer wg.Done()
		defer client.mutex.Unlock()
		echo = resp.Data
	})
	wg.Wait()
	return
}

// Close
func (client *Client) Close() (err error) {
	client.isConn = false
	return client.conn.Close()
}
