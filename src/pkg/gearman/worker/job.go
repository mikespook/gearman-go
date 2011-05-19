package gearman

import (
    "os"
    "log"
)

type Job struct {
    Data []byte
    Handle string
    UniqueId string
    client *jobClient
    magicCode, dataType uint32
}

func byteToUint32(buf [4]byte) uint32 {
    return uint32(buf[0]) << 24 +
        uint32(buf[1]) << 16 +
        uint32(buf[2]) << 8 +
        uint32(buf[3])
}

func uint32ToByte(i uint32) (data [4]byte) {
    data[0] = byte((i >> 24) & 0xff)
    data[1] = byte((i >> 16) & 0xff)
    data[2] = byte((i >> 8) & 0xff)
    data[3] = byte(i & 0xff)
    return
}

func NewJob(magiccode, datatype uint32, data []byte) (job *Job) {
    return &Job{magicCode:magiccode,
        dataType: datatype,
        Data:data}
}

func DecodeJob(data []byte) (job *Job, err os.Error) {
    if len(data) < 12 {
        return nil, os.NewError("Data length is too small.")
    }
    datatype := byteToUint32([4]byte{data[4], data[5], data[6], data[7]})
    l := byteToUint32([4]byte{data[8], data[9], data[10], data[11]})
    if len(data[12:]) != int(l) {
         return nil, os.NewError("Invalid data length.")
    }
    data = data[12:]
    return NewJob(REQ, datatype, data), err
}

func (job *Job) Encode() (data []byte) {
    magiccode := uint32ToByte(job.magicCode)
    datatype := uint32ToByte(job.dataType)
    data = make([]byte, 0, 1024 * 64)
    data = append(data, magiccode[:] ...)
    data = append(data, datatype[:] ...)
    data = append(data, []byte{0, 0, 0, 0} ...)
    l := len(job.Data)
    if job.Handle != "" {
        data = append(data, []byte(job.Handle) ...)
        data = append(data, 0)
        l += len(job.Handle) + 1
    }
    data = append(data, job.Data ...)
    datalength := uint32ToByte(uint32(l))
    copy(data[8:12], datalength[:])
    log.Println(data)
    return
}

// update data
func (job * Job) UpdateData(data []byte, iswaring bool) (err os.Error) {
    result := append([]byte(job.Handle), 0)
    result = append(result, data ...)
    var datatype uint32
    if iswaring {
        datatype = WORK_WARNING
    } else {
        datatype = WORK_DATA
    }
    return job.client.WriteJob(NewJob(REQ, datatype, result))
}

// update status
func (job * Job) UpdateStatus(numerator, denominator uint32) (err os.Error) {
    n := uint32ToByte(numerator)
    d := uint32ToByte(denominator)
    result := append([]byte(job.Handle), 0)
    result = append(result, n[:] ...)
    result = append(result, d[:] ...)
    return job.client.WriteJob(NewJob(REQ, WORK_STATUS, result))
}


