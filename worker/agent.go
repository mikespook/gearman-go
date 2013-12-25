package worker

import (
	"io"
	"net"
)

// The agent of job server.
type agent struct {
	conn      net.Conn
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
		in:     make(chan []byte, QUEUE_SIZE),
	}
	return
}

func (a *agent) Connect() (err error) {
	a.conn, err = net.Dial(a.net, a.addr)
	if err != nil {
		return
	}
	go a.work()
	return
}

func (a *agent) work() {
	var inpack *inPack
	var l int
	var err error
	var data, leftdata []byte
	for {
		if data, err = a.read(BUFFER_SIZE); err != nil {
			if err == ErrConnClosed {
				break
			}
			a.worker.err(err)
			continue
		}
		if len(leftdata) > 0 { // some data left for processing
			data = append(leftdata, data...)
		}
		if len(data) < MIN_PACKET_LEN { // not enough data
			leftdata = data
			continue
		}
		if inpack, l, err = decodeInPack(data); err != nil {
			a.worker.err(err)
			continue
		}
		leftdata = nil
		inpack.a = a
		a.worker.in <- inpack
		if len(data) > l {
			leftdata = data[l:]
		}
	}
}

func (a *agent) Close() {
	a.conn.Close()
}

func (a *agent) Grab() {
	outpack := getOutPack()
	outpack.dataType = GRAB_JOB_UNIQ
	a.write(outpack)
}

func (a *agent) PreSleep() {
	outpack := getOutPack()
	outpack.dataType = PRE_SLEEP
	a.write(outpack)
}

// read length bytes from the socket
func (a *agent) read(length int) (data []byte, err error) {
	n := 0
	buf := getBuffer(BUFFER_SIZE)
	// read until data can be unpacked
	for i := length; i > 0 || len(data) < MIN_PACKET_LEN; i -= n {
		if n, err = a.conn.Read(buf); err != nil {
			if err == io.EOF && n == 0 {
				if data == nil {
					err = ErrConnection
				} else {
					err = ErrConnClosed
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
func (a *agent) write(outpack *outPack) (err error) {
	var n int
	buf := outpack.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = a.conn.Write(buf[i:])
		if err != nil {
			return err
		}
	}
	return
}
