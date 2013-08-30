// Copyright 2011 Xing Xing <mikespook@gmail.com>
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
	"encoding/binary"
)

// Worker side job
type request struct {
	DataType             uint32
	Data                 []byte
	Handle, UniqueId, Fn string
}

func getRequest() (req *request) {
	// TODO pool
	return &request{}
}

// Encode a job to byte slice
func (req *request) Encode() (data []byte) {
	var l int
	if req.DataType == WORK_FAIL {
		l = len(req.Handle)
	} else {
		l = len(req.Data)
		if req.Handle != "" {
			l += len(req.Handle) + 1
		}
	}
	data = getBuffer(l + MIN_PACKET_LEN)
	binary.BigEndian.PutUint32(data[:4], REQ)
	binary.BigEndian.PutUint32(data[4:8], req.DataType)
	binary.BigEndian.PutUint32(data[8:MIN_PACKET_LEN], uint32(l))
	i := MIN_PACKET_LEN
	if req.Handle != "" {
		hi := len(req.Handle) + i
		copy(data[i:hi], []byte(req.Handle))
		if req.DataType != WORK_FAIL {
			data[hi] = '\x00'
		}
		i = i + hi
	}
	copy(data[i:], req.Data)
	return
}
