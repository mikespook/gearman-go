// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
	"io"
	"net"
)

// The agent of job server.
type agent struct {
	conn   net.Conn
	worker *Worker
	in     chan []byte
	net, addr   string
	isConn bool
}

// Create the agent of job server.
func newAgent(net, addr string, worker *Worker) (a *agent, err error) {
	a = &agent{
		net: net,
		addr:   addr,
		worker: worker,
		in:     make(chan []byte, QUEUE_SIZE),
	}
	return
}

func (a *agent) Connect() (err error) {
	a.conn, err = net.Dial(a.net, a.addr)
	if err != nil {
		return
	}
	a.isConn = true
	return
}

func (a *agent) Work() {
	go a.readLoop()

	var resp *Response
	var l int
	var err error
	var data, leftdata []byte
	for data = range a.in {
		if len(leftdata) > 0 { // some data left for processing
			data = append(leftdata, data...)
		}
		l = len(data)
		if l < MIN_PACKET_LEN { // not enough data
			leftdata = data
			continue
		}
		if resp, l, err = decodeResponse(data); err != nil {
			a.worker.err(err)
			continue
		}
		leftdata = nil
		resp.agentId = a.net + a.addr
		a.worker.in <- resp
		if len(data) > l {
			leftdata = data[l:]
		}
	}
}

// read data from socket
func (a *agent) readLoop() {
	var data []byte
	var err error
	for a.isConn {
		if data, err = a.read(BUFFER_SIZE); err != nil {
			if err == ErrConnClosed {
				break
			}
			a.worker.err(err)
			continue
		}
		a.in <- data
	}
	close(a.in)
}

func (a *agent) Close() {
	a.conn.Close()
}

// read length bytes from the socket
func (a *agent) read(length int) (data []byte, err error) {
	n := 0
	buf := getBuffer(BUFFER_SIZE)
	// read until data can be unpacked
	for i := length; i > 0 || len(data) < MIN_PACKET_LEN; i -= n {
		if n, err = a.conn.Read(buf); err != nil {
			if !a.isConn {
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

// Internal write the encoded job.
func (a *agent) write(req *request) (err error) {
	var n int
	buf := req.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = a.conn.Write(buf[i:])
		if err != nil {
			return err
		}
	}
	return
}
