// The client package helps developers connect to Gearmand, send
// jobs and fetch result.
package client

import (
	"bufio"
	"net"
	"sync"
	"time"
)

var (
	DefaultTimeout time.Duration = time.Second
)

// One client connect to one server.
// Use Pool for multi-connections.
type Client struct {
	sync.Mutex

	net, addr    string
	innerHandler *responseHandlerMap
	in           chan *Response
	conn         net.Conn
	rw           *bufio.ReadWriter

	ResponseTimeout time.Duration // response timeout for do()

	ErrorHandler ErrorHandler
}

type responseHandlerMap struct {
	sync.Mutex
	holder map[string]handledResponse
}

type handledResponse struct {
	internal ResponseHandler // internal handler, always non-nil
	external ResponseHandler // handler passed in from (*Client).Do, sometimes nil
}

func newResponseHandlerMap() *responseHandlerMap {
	return &responseHandlerMap{holder: make(map[string]handledResponse, queueSize)}
}

func (r *responseHandlerMap) remove(key string) {
	r.Lock()
	delete(r.holder, key)
	r.Unlock()
}

func (r *responseHandlerMap) getAndRemove(key string) (handledResponse, bool) {
	r.Lock()
	rh, b := r.holder[key]
	delete(r.holder, key)
	r.Unlock()
	return rh, b
}

func (r *responseHandlerMap) putWithExternalHandler(key string, internal, external ResponseHandler) {
	r.Lock()
	r.holder[key] = handledResponse{internal: internal, external: external}
	r.Unlock()
}

func (r *responseHandlerMap) put(key string, rh ResponseHandler) {
	r.putWithExternalHandler(key, rh, nil)
}

// New returns a client.
func New(network, addr string) (client *Client, err error) {
	client = &Client{
		net:             network,
		addr:            addr,
		innerHandler:    newResponseHandlerMap(),
		in:              make(chan *Response, queueSize),
		ResponseTimeout: DefaultTimeout,
	}
	client.conn, err = net.Dial(client.net, client.addr)
	if err != nil {
		return
	}
	client.rw = bufio.NewReadWriter(bufio.NewReader(client.conn),
		bufio.NewWriter(client.conn))
	go client.readLoop()
	go client.processLoop()
	return
}

func (client *Client) write(req *request) (err error) {
	var n int
	buf := req.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = client.rw.Write(buf[i:])
		if err != nil {
			return
		}
	}
	return client.rw.Flush()
}

func (client *Client) read(length int) (data []byte, err error) {
	n := 0
	buf := getBuffer(bufferSize)
	// read until data can be unpacked
	for i := length; i > 0 || len(data) < minPacketLength; i -= n {
		if n, err = client.rw.Read(buf); err != nil {
			return
		}
		data = append(data, buf[0:n]...)
		if n < bufferSize {
			break
		}
	}
	return
}

func (client *Client) readLoop() {
	defer close(client.in)
	var data, leftdata []byte
	var err error
	var resp *Response
ReadLoop:
	for client.conn != nil {
		if data, err = client.read(bufferSize); err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Timeout() {
					client.err(err)
				}
				if opErr.Temporary() {
					continue
				}
				break
			}
			client.err(err)
			// If it is unexpected error and the connection wasn't
			// closed by Gearmand, the client should close the conection
			// and reconnect to job server.
			client.Close()
			client.conn, err = net.Dial(client.net, client.addr)
			if err != nil {
				client.err(err)
				break
			}
			client.rw = bufio.NewReadWriter(bufio.NewReader(client.conn),
				bufio.NewWriter(client.conn))
			continue
		}
		if len(leftdata) > 0 { // some data left for processing
			data = append(leftdata, data...)
			leftdata = nil
		}
		for {
			l := len(data)
			if l < minPacketLength { // not enough data
				leftdata = data
				continue ReadLoop
			}
			if resp, l, err = decodeResponse(data); err != nil {
				leftdata = data[l:]
				continue ReadLoop
			} else {
				client.in <- resp
			}
			data = data[l:]
			if len(data) > 0 {
				continue
			}
			break
		}
	}
}

func (client *Client) processLoop() {
	rhandlers := map[string]ResponseHandler{}
	for resp := range client.in {
		switch resp.DataType {
		case dtError:
			client.err(getError(resp.Data))
		case dtStatusRes:
			client.handleInner("s"+resp.Handle, resp, nil)
		case dtJobCreated:
			client.handleInner("c", resp, rhandlers)
		case dtEchoRes:
			client.handleInner("e", resp, nil)
		case dtWorkData, dtWorkWarning, dtWorkStatus:
			if cb := rhandlers[resp.Handle]; cb != nil {
				cb(resp)
			}
		case dtWorkComplete, dtWorkFail, dtWorkException:
			if cb := rhandlers[resp.Handle]; cb != nil {
				cb(resp)
				delete(rhandlers, resp.Handle)
			}
		}
	}
}

func (client *Client) err(e error) {
	if client.ErrorHandler != nil {
		client.ErrorHandler(e)
	}
}

func (client *Client) handleInner(key string, resp *Response, rhandlers map[string]ResponseHandler) {
	if h, ok := client.innerHandler.getAndRemove(key); ok {
		if h.external != nil && resp.Handle != "" {
			rhandlers[resp.Handle] = h.external
		}
		h.internal(resp)
	}
}

type handleOrError struct {
	handle string
	err    error
}

func (client *Client) do(funcname string, data []byte,
	flag uint32, h ResponseHandler, id string) (handle string, err error) {
	if len(id) == 0 {
		return "", ErrInvalidId
	}
	if client.conn == nil {
		return "", ErrLostConn
	}
	var result = make(chan handleOrError, 1)
	client.Lock()
	defer client.Unlock()
	client.innerHandler.putWithExternalHandler("c", func(resp *Response) {
		if resp.DataType == dtError {
			err = getError(resp.Data)
			result <- handleOrError{"", err}
			return
		}
		handle = resp.Handle
		result <- handleOrError{handle, nil}
	}, h)
	req := getJob(id, []byte(funcname), data)
	req.DataType = flag
	if err = client.write(req); err != nil {
		client.innerHandler.remove("c")
		return
	}
	var timer = time.After(client.ResponseTimeout)
	select {
	case ret := <-result:
		return ret.handle, ret.err
	case <-timer:
		client.innerHandler.remove("c")
		return "", ErrLostConn
	}
	return
}

// Call the function and get a response.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) Do(funcname string, data []byte,
	flag byte, h ResponseHandler) (handle string, err error) {
	handle, err = client.DoWithId(funcname, data, flag, h, IdGen.Id())
	return
}

// Call the function in background, no response needed.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoBg(funcname string, data []byte,
	flag byte) (handle string, err error) {
	handle, err = client.DoBgWithId(funcname, data, flag, IdGen.Id())
	return
}

// Status gets job status from job server.
func (client *Client) Status(handle string) (status *Status, err error) {
	if client.conn == nil {
		return nil, ErrLostConn
	}
	var mutex sync.Mutex
	mutex.Lock()
	client.innerHandler.put("s"+handle, func(resp *Response) {
		defer mutex.Unlock()
		var err error
		status, err = resp._status()
		if err != nil {
			client.err(err)
		}
	})
	req := getRequest()
	req.DataType = dtGetStatus
	req.Data = []byte(handle)
	client.write(req)
	mutex.Lock()
	return
}

// Echo.
func (client *Client) Echo(data []byte) (echo []byte, err error) {
	if client.conn == nil {
		return nil, ErrLostConn
	}
	var mutex sync.Mutex
	mutex.Lock()

	// Lock the client as only one echo may run parallel
	client.Lock()
	defer client.Unlock()
	
	client.innerHandler.put("e", func(resp *Response) {
		echo = resp.Data
		mutex.Unlock()
	})
	req := getRequest()
	req.DataType = dtEchoReq
	req.Data = data
	client.write(req)
	mutex.Lock()
	return
}

// Close connection
func (client *Client) Close() (err error) {
	client.Lock()
	defer client.Unlock()
	if client.conn != nil {
		err = client.conn.Close()
		client.conn = nil
	}
	return
}

// Call the function and get a response.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoWithId(funcname string, data []byte,
	flag byte, h ResponseHandler, id string) (handle string, err error) {
	var datatype uint32
	switch flag {
	case JobLow:
		datatype = dtSubmitJobLow
	case JobHigh:
		datatype = dtSubmitJobHigh
	default:
		datatype = dtSubmitJob
	}
	handle, err = client.do(funcname, data, datatype, h, id)
	return
}

// Call the function in background, no response needed.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoBgWithId(funcname string, data []byte,
	flag byte, id string) (handle string, err error) {
	if client.conn == nil {
		return "", ErrLostConn
	}
	var datatype uint32
	switch flag {
	case JobLow:
		datatype = dtSubmitJobLowBg
	case JobHigh:
		datatype = dtSubmitJobHighBg
	default:
		datatype = dtSubmitJobBg
	}
	handle, err = client.do(funcname, data, datatype, nil, id)
	return
}
