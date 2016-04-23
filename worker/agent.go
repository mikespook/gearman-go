package worker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
)

// The agent of job server.
type agent struct {
	sync.Mutex
	conn      net.Conn
	rw        *bufio.ReadWriter
	worker    *Worker
	in        chan []byte
	net, addr string
}

// Create the agent of job server.
func newAgent(net, addr string, worker *Worker) (a *agent, err error) {
	a = &agent{
		net:    net,
		addr:   addr,
		worker: worker,
		in:     make(chan []byte, queueSize),
	}
	return
}

func (a *agent) Connect() (err error) {
	a.Lock()
	defer a.Unlock()
	a.conn, err = net.Dial(a.net, a.addr)
	if err != nil {
		return
	}
	a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
		bufio.NewWriter(a.conn))
	go a.work()
	return
}

func (a *agent) work() {
	defer func() {
		if err := recover(); err != nil {
			a.worker.err(err.(error))
		}
	}()

	var inpack *inPack
	var l int
	var err error
	var data, leftdata []byte
	for {
		if data, err = a.read(); err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Temporary() {
					continue
				} else {
					a.disconnect_error(err)
					// else - we're probably dc'ing due to a Close()

					break
				}

			} else if err == io.EOF {
				a.disconnect_error(err)
				break
			}
			a.worker.err(err)
			// If it is unexpected error and the connection wasn't
			// closed by Gearmand, the agent should close the conection
			// and reconnect to job server.
			a.Close()
			a.conn, err = net.Dial(a.net, a.addr)
			if err != nil {
				a.worker.err(err)
				break
			}
			a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
				bufio.NewWriter(a.conn))
		}
		if len(leftdata) > 0 { // some data left for processing
			data = append(leftdata, data...)
		}
		if len(data) < minPacketLength { // not enough data
			leftdata = data
			continue
		}
		for {
			if inpack, l, err = decodeInPack(data); err != nil {
				a.worker.err(err)
				leftdata = data
				break
			} else {
				leftdata = nil
				inpack.a = a
				a.worker.in <- inpack
				if len(data) == l {
					break
				}
				if len(data) > l {
					data = data[l:]
				}
			}
		}
	}
}

func (a *agent) disconnect_error(err error) {
	if a.conn != nil {
		err = &WorkerDisconnectError{
			err:   err,
			agent: a,
		}
		a.worker.err(err)
	}
}

func (a *agent) Close() {
	a.Lock()
	defer a.Unlock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
}

func (a *agent) Grab() {
	a.Lock()
	defer a.Unlock()
	a.grab()
}

func (a *agent) grab() {
	outpack := getOutPack()
	outpack.dataType = dtGrabJobUniq
	a.write(outpack)
}

func (a *agent) PreSleep() {
	a.Lock()
	defer a.Unlock()
	outpack := getOutPack()
	outpack.dataType = dtPreSleep
	a.write(outpack)
}

func (a *agent) reconnect() error {
	a.Lock()
	defer a.Unlock()
	conn, err := net.Dial(a.net, a.addr)
	if err != nil {
		return err
	}
	a.conn = conn
	a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
		bufio.NewWriter(a.conn))
	a.grab()
	a.worker.reRegisterFuncsForAgent(a)

	go a.work()
	return nil
}

// read length bytes from the socket
func (a *agent) read() (data []byte, err error) {
	n := 0

	tmp := getBuffer(bufferSize)
	var buf bytes.Buffer

	// read the header so we can get the length of the data
	if n, err = a.rw.Read(tmp); err != nil {
		return
	}
	dl := int(binary.BigEndian.Uint32(tmp[8:12]))

	// write what we read so far
	buf.Write(tmp[:n])

	// read until we receive all the data
	for buf.Len() < dl+minPacketLength {
		if n, err = a.rw.Read(tmp); err != nil {
			return buf.Bytes(), err
		}

		buf.Write(tmp[:n])
	}

	return buf.Bytes(), err
}

// Internal write the encoded job.
func (a *agent) write(outpack *outPack) (err error) {
	var n int
	buf := outpack.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = a.rw.Write(buf[i:])
		if err != nil {
			return err
		}
	}
	return a.rw.Flush()
}

// Write with lock
func (a *agent) Write(outpack *outPack) (err error) {
	a.Lock()
	defer a.Unlock()
	return a.write(outpack)
}
