package gearman

import (
    "os"
)

type ClientJob struct {
    Data []byte
    Handle string
    UniqueId string
    magicCode, dataType uint32
}

func NewClientJob(magiccode, datatype uint32, data []byte) (job *ClientJob) {
    return &ClientJob{magicCode:magiccode,
        dataType:datatype,
        Data:data}
}

func DecodeClientJob(data []byte) (job * ClientJob, err os.Error) {
    if len(data) < 12 {
        err = os.NewError("Data length is too small.")
        return
    }
    datatype := byteToUint32([4]byte{data[4], data[5], data[6], data[7]})
    l := byteToUint32([4]byte{data[8], data[9], data[10], data[11]})
    if len(data[12:]) != int(l) {
         err = os.NewError("Invalid data length.")
         return
    }
    data = data[12:]
    job = NewClientJob(RES, datatype, data)
    return
}

func (job *ClientJob) Encode() (data []byte) {
    magiccode := uint32ToByte(job.magicCode)
    datatype := uint32ToByte(job.dataType)
    data = make([]byte, 0, 1024 * 64)
    data = append(data, magiccode[:] ...)
    data = append(data, datatype[:] ...)
    data = append(data, []byte{0, 0, 0, 0} ...)
    l := len(job.Data)
    data = append(data, job.Data ...)
    datalength := uint32ToByte(uint32(l))
    copy(data[8:12], datalength[:])
    return
}

