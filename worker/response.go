// Copyright 2011 Xing Xing <mikespook@gmail.com>
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"encoding/binary"
)

// Worker side job
type Response struct {
	DataType  uint32
	Data                 []byte
	Handle, UniqueId, Fn string
	agentId	string
}

// Create a new job
func getResponse() (resp *Response) {
	return &Response{}
}

// Decode job from byte slice
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
	case JOB_ASSIGN:
		s := bytes.SplitN(dt, []byte{'\x00'}, 3)
		if len(s) == 3 {
			resp.Handle = string(s[0])
			resp.Fn = string(s[1])
			resp.Data = s[2]
		}
	case JOB_ASSIGN_UNIQ:
		s := bytes.SplitN(dt, []byte{'\x00'}, 4)
		if len(s) == 4 {
			resp.Handle = string(s[0])
			resp.Fn = string(s[1])
			resp.UniqueId = string(s[2])
			resp.Data = s[3]
		}
	default:
		resp.Data = dt
	}
	l = dl + MIN_PACKET_LEN
	return
}
