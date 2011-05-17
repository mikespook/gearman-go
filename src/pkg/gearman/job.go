package gearman

import (
    "os"
)

const (
    // \x00REQ
    REQ = 5391697
    // \x00RES
    RES = 5391699
    
    CAN_DO = 1
    CANT_DO = 2
    ECHO_REQ = 16
    ECHO_RES = 17
    ERROR = 19
    CAN_DO_TIMEOUT = 23
)

type Job struct {
    client *JobClient
    Data []byte
    MagicCode, DataType uint32
}

func ByteToUint32(buf [4]byte) uint32 {
    return uint32(buf[0]) << 24 +
        uint32(buf[1]) << 16 +
        uint32(buf[2]) << 8 +
        uint32(buf[3])
}

func Uint32ToByte(i uint32) (data [4]byte) {
    data[0] = byte((i >> 24) & 0xff)
    data[1] = byte((i >> 16) & 0xff)
    data[2] = byte((i >> 8) & 0xff)
    data[3] = byte(i & 0xff)
    return
}

func NewJob(server *JobClient, magiccode, datatype uint32, data []byte) (job *Job) {
    return &Job{client: server,
        MagicCode:magiccode,
        DataType: datatype,
        Data:data}
}

func DecodeJob(server *JobClient, data []byte) (job *Job, err os.Error) {
    if len(data) < 12 {
        return nil, os.NewError("Data length is too small.")
    }
    datatype := ByteToUint32([4]byte{data[4], data[5], data[6], data[7]})
    l := ByteToUint32([4]byte{data[8], data[9], data[10], data[11]})
    if len(data[12:]) != int(l) {
         return nil, os.NewError("Invalid data length.")
    }
    switch(ByteToUint32([4]byte{data[4], data[5], data[6], data[7]})) {
        case ECHO_RES:
            data = data[12:]
    }
    return NewJob(server, REQ, datatype, data), err
}

func (job *Job) Encode() (data []byte) {
    magiccode := Uint32ToByte(job.MagicCode)
    datatype := Uint32ToByte(job.DataType)
    l := len(job.Data)
    datalength := Uint32ToByte(uint32(l))
    data = make([]byte, 12 + l)
    copy(data[:4], magiccode[:])
    copy(data[4:8], datatype[:])
    copy(data[8:12], datalength[:])
    copy(data[12:], job.Data)
    return
}



