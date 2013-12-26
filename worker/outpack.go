package worker

import (
	"encoding/binary"
)

// Worker side job
type outPack struct {
	dataType uint32
	data     []byte
	handle   string
}

func getOutPack() (outpack *outPack) {
	// TODO pool
	return &outPack{}
}

// Encode a job to byte slice
func (outpack *outPack) Encode() (data []byte) {
	var l int
	if outpack.dataType == dtWorkFail {
		l = len(outpack.handle)
	} else {
		l = len(outpack.data)
		if outpack.handle != "" {
			l += len(outpack.handle) + 1
		}
	}
	data = getBuffer(l + minPacketLength)
	binary.BigEndian.PutUint32(data[:4], req)
	binary.BigEndian.PutUint32(data[4:8], outpack.dataType)
	binary.BigEndian.PutUint32(data[8:minPacketLength], uint32(l))
	i := minPacketLength
	if outpack.handle != "" {
		hi := len(outpack.handle) + i
		copy(data[i:hi], []byte(outpack.handle))
		if outpack.dataType != dtWorkFail {
			data[hi] = '\x00'
		}
		i = hi + 1
	}
	if outpack.dataType != dtWorkFail {
		copy(data[i:], outpack.data)
	}
	return
}
