// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package gearman

import (
    "os"
    "strconv"
)

// Worker side job
type WorkerJob struct {
    Data                []byte
    Handle, UniqueId    string
    agent              *jobAgent
    magicCode, DataType uint32
}

// Create a new job
func NewWorkerJob(magiccode, datatype uint32, data []byte) (job *WorkerJob) {
    return &WorkerJob{magicCode: magiccode,
        DataType: datatype,
        Data:     data}
}

// Decode job from byte slice
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

// Encode a job to byte slice
func (job *WorkerJob) Encode() (data []byte) {
    magiccode := uint32ToByte(job.magicCode)
    datatype := uint32ToByte(job.DataType)
    data = make([]byte, 0, 1024*64)
    data = append(data, magiccode[:]...)
    data = append(data, datatype[:]...)
    data = append(data, []byte{0, 0, 0, 0}...)
    l := len(job.Data)
    if job.Handle != "" {
        data = append(data, []byte(job.Handle)...)
        data = append(data, 0)
        l += len(job.Handle) + 1
    }
    data = append(data, job.Data...)
    datalength := uint32ToByte(uint32(l))
    copy(data[8:12], datalength[:])
    return
}

// Send some datas to client.
// Using this in a job's executing.
func (job *WorkerJob) UpdateData(data []byte, iswaring bool) (err os.Error) {
    result := append([]byte(job.Handle), 0)
    result = append(result, data...)
    var datatype uint32
    if iswaring {
        datatype = WORK_WARNING
    } else {
        datatype = WORK_DATA
    }
    return job.agent.WriteJob(NewWorkerJob(REQ, datatype, result))
}

// Update status.
// Tall client how many percent job has been executed.
func (job *WorkerJob) UpdateStatus(numerator, denominator int) (err os.Error) {
    n := []byte(strconv.Itoa(numerator))
    d := []byte(strconv.Itoa(denominator))
    result := append([]byte(job.Handle), 0)
    result = append(result, n...)
    result = append(result, d...)
    return job.agent.WriteJob(NewWorkerJob(REQ, WORK_STATUS, result))
}
