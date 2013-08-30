// Copyright 2011 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

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

// Extract the Response's result.
// if data == nil, err != nil, then worker failing to execute job
// if data != nil, err != nil, then worker has a exception
// if data != nil, err == nil, then worker complate job
// after calling this method, the Response.Handle will be filled
func (resp *Response) Result() (data []byte, err error) {
	switch resp.DataType {
	case WORK_FAIL:
		resp.Handle = string(resp.Data)
		err = ErrWorkFail
		return
	case WORK_EXCEPTION:
		err = ErrWorkException
		fallthrough
	case WORK_COMPLETE:
		s := bytes.SplitN(resp.Data, []byte{'\x00'}, 2)
		if len(s) != 2 {
			err = fmt.Errorf("Invalid data: %V", resp.Data)
			return
		}
		resp.Handle = string(s[0])
		data = s[1]
	default:
		err = ErrDataType
	}
	return
}

// Extract the job's update
func (resp *Response) Update() (data []byte, err error) {
	if resp.DataType != WORK_DATA &&
		resp.DataType != WORK_WARNING {
		err = ErrDataType
		return
	}
	s := bytes.SplitN(resp.Data, []byte{'\x00'}, 2)
	if len(s) != 2 {
		err = ErrInvalidData
		return
	}
	if resp.DataType == WORK_WARNING {
		err = ErrWorkWarning
	}
	resp.Handle = string(s[0])
	data = s[1]
	return
}

// Decode a job from byte slice
func decodeResponse(data []byte) (resp *Response, l int, err error) {
	if len(data) < MIN_PACKET_LEN { // valid package should not less 12 bytes
		err = fmt.Errorf("Invalid data: %V", data)
		return
	}
	dl := int(binary.BigEndian.Uint32(data[8:12]))
	dt := data[MIN_PACKET_LEN : dl+MIN_PACKET_LEN]
	if len(dt) != int(dl) { // length not equal
		err = fmt.Errorf("Invalid data: %V", data)
		return
	}
	resp = getResponse()
	resp.DataType = binary.BigEndian.Uint32(data[4:8])
	switch resp.DataType {
	case JOB_CREATED:
		resp.Handle = string(dt)
	case STATUS_RES, WORK_DATA, WORK_WARNING, WORK_STATUS,
		WORK_COMPLETE, WORK_FAIL, WORK_EXCEPTION:
		s := bytes.SplitN(dt, []byte{'\x00'}, 2)
		if len(s) >= 2 {
			resp.Handle = string(s[0])
			resp.Data = s[1]
		} else {
			err = fmt.Errorf("Invalid data: %V", data)
			return
		}
	case ECHO_RES:
		fallthrough
	default:
		resp.Data = dt
	}
	l = dl + MIN_PACKET_LEN
	return
}

func (resp *Response) IsEcho() bool {
	return resp.DataType == ECHO_RES
}

func (resp *Response) IsStatus() bool {
	return resp.DataType == STATUS_RES
}

// status handler
func (resp *Response) Status() (status *Status, err error) {
	data := bytes.SplitN(resp.Data, []byte{'\x00'}, 4)
	if len(data) != 4 {
		err = fmt.Errorf("Invalid data: %V", resp.Data)
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
