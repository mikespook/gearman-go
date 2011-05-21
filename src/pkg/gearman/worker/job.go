package gearman

import (
    "os"
//    "log"
)

type WorkerJob struct {
    Data []byte
    Handle, UniqueId string
    client *jobClient
    magicCode, DataType uint32
    Job
}

func NewWorkerJob(magiccode, datatype uint32, data []byte) (job *WorkerJob) {
    return &WorkerJob{magicCode:magiccode,
        DataType: datatype,
        Data:data}
}

func DecodeWorkerJob(data []byte) (job *WorkerJob, err os.Error) {
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
    job = NewWorkerJob(RES, datatype, data)
    return
}

func (job *WorkerJob) Encode() (data []byte) {
    magiccode := uint32ToByte(job.magicCode)
    datatype := uint32ToByte(job.DataType)
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
    return
}

// update data
func (job * WorkerJob) UpdateData(data []byte, iswaring bool) (err os.Error) {
    result := append([]byte(job.Handle), 0)
    result = append(result, data ...)
    var datatype uint32
    if iswaring {
        datatype = WORK_WARNING
    } else {
        datatype = WORK_DATA
    }
    return job.client.WriteJob(NewWorkerJob(REQ, datatype, result))
}

// update status
func (job * WorkerJob) UpdateStatus(numerator, denominator uint32) (err os.Error) {
    n := uint32ToByte(numerator)
    d := uint32ToByte(denominator)
    result := append([]byte(job.Handle), 0)
    result = append(result, n[:] ...)
    result = append(result, d[:] ...)
    return job.client.WriteJob(NewWorkerJob(REQ, WORK_STATUS, result))
}


