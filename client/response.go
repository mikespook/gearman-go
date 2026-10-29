package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
)

// Response handler
type ResponseHandler func(*Response)

// response
type Response struct {
	DataType  uint32
	Data, UID []byte
	Handle    string
}

// Result extracts the outcome of a terminal response:
//
//	WORK_COMPLETE:  data = the job's result,           err = nil
//	WORK_FAIL:      data = nil,                        err = ErrWorkFail
//	WORK_EXCEPTION: data = the worker's exception payload (non-nil, possibly
//	                empty),                            err = ErrWorkException
//
// Any other DataType yields ErrDataType. Response.Handle was already filled in
// when the packet was decoded and is left untouched.
//
// A WORK_EXCEPTION only reaches the client when the "exceptions" option was
// negotiated on the connection; see DefaultExceptions. Without it the job
// server downgrades the worker's exception to a WORK_FAIL, so the same failure
// arrives as ErrWorkFail with no payload.
func (resp *Response) Result() (data []byte, err error) {
	switch resp.DataType {
	case dtWorkFail:
		err = ErrWorkFail
		return
	case dtWorkException:
		err = ErrWorkException
		fallthrough
	case dtWorkComplete:
		data = resp.Data
	default:
		err = ErrDataType
	}
	return
}

// Extract the job's update
func (resp *Response) Update() (data []byte, err error) {
	if resp.DataType != dtWorkData &&
		resp.DataType != dtWorkWarning {
		err = ErrDataType
		return
	}
	data = resp.Data
	if resp.DataType == dtWorkWarning {
		err = ErrWorkWarning
	}
	return
}

// Decode a job from byte slice
func decodeResponse(data []byte) (resp *Response, l int, err error) {
	a := len(data)
	if a < minPacketLength { // valid package should not less 12 bytes
		err = errIncompletePacket
		return
	}
	dl := int(binary.BigEndian.Uint32(data[8:12]))
	if a < minPacketLength+dl {
		err = errIncompletePacket
		return
	}
	dt := data[minPacketLength : dl+minPacketLength]
	resp = getResponse()
	resp.DataType = binary.BigEndian.Uint32(data[4:8])
	switch resp.DataType {
	case dtJobCreated:
		resp.Handle = string(dt)
	case dtStatusRes, dtWorkData, dtWorkWarning, dtWorkStatus,
		dtWorkComplete, dtWorkException:
		s := bytes.SplitN(dt, []byte{'\x00'}, 2)
		if len(s) >= 2 {
			resp.Handle = string(s[0])
			resp.Data = s[1]
		} else {
			err = fmt.Errorf("Invalid data: %v", data)
			return
		}
	case dtWorkFail:
		s := bytes.SplitN(dt, []byte{'\x00'}, 2)
		if len(s) >= 1 {
			resp.Handle = string(s[0])
		} else {
			err = fmt.Errorf("Invalid data: %v", data)
			return
		}
	case dtEchoRes, dtOptionRes:
		// Both carry a single opaque argument and no job handle: the echoed
		// payload, respectively the name of the option that was set.
		resp.Data = dt
	default:
		resp.Data = dt
	}
	l = dl + minPacketLength
	return
}

func (resp *Response) Status() (status *Status, err error) {
	data := bytes.SplitN(resp.Data, []byte{'\x00'}, 2)
	if len(data) != 2 {
		err = fmt.Errorf("Invalid data: %v", resp.Data)
		return
	}
	status = &Status{}
	status.Handle = resp.Handle
	status.Known = true
	status.Running = true
	status.Numerator, err = strconv.ParseUint(string(data[0]), 10, 0)
	if err != nil {
		err = fmt.Errorf("Invalid Integer: %s", data[0])
		return
	}
	status.Denominator, err = strconv.ParseUint(string(data[1]), 10, 0)
	if err != nil {
		err = fmt.Errorf("Invalid Integer: %s", data[1])
		return
	}
	return
}

// status handler
func (resp *Response) _status() (status *Status, err error) {
	data := bytes.SplitN(resp.Data, []byte{'\x00'}, 4)
	if len(data) != 4 {
		err = fmt.Errorf("Invalid data: %v", resp.Data)
		return
	}
	status = &Status{}
	status.Handle = resp.Handle
	status.Known = (data[0][0] == '1')
	status.Running = (data[1][0] == '1')
	status.Numerator, err = strconv.ParseUint(string(data[2]), 10, 0)
	if err != nil {
		err = fmt.Errorf("Invalid Integer: %s", data[2])
		return
	}
	status.Denominator, err = strconv.ParseUint(string(data[3]), 10, 0)
	if err != nil {
		err = fmt.Errorf("Invalid Integer: %s", data[3])
		return
	}
	return
}

func getResponse() (resp *Response) {
	// TODO add a pool
	resp = &Response{}
	return
}
