// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

import (
    "bitbucket.org/mikespook/gearman-go/gearman"
    "bytes"
)

// Client side job
type ClientJob struct {
    Data                []byte
    Handle, UniqueId    string
    magicCode, DataType uint32
}

// Create a new job
func NewClientJob(magiccode, datatype uint32, data []byte) (job *ClientJob) {
    return &ClientJob{magicCode: magiccode,
        DataType: datatype,
        Data:     data}
}

// Decode a job from byte slice
func DecodeClientJob(data []byte) (job *ClientJob, err error) {
    if len(data) < 12 {
        err = gearman.ErrInvalidData
        return
    }
    datatype := gearman.BytesToUint32([4]byte{data[4], data[5], data[6], data[7]})
    l := gearman.BytesToUint32([4]byte{data[8], data[9], data[10], data[11]})
    if len(data[12:]) != int(l) {
        err = gearman.ErrInvalidData
        return
    }
    data = data[12:]
    job = NewClientJob(gearman.RES, datatype, data)
    return
}

// Encode a job to byte slice
func (job *ClientJob) Encode() (data []byte) {
    magiccode := gearman.Uint32ToBytes(job.magicCode)
    datatype := gearman.Uint32ToBytes(job.DataType)
    data = make([]byte, 0, 1024*64)
    data = append(data, magiccode[:]...)
    data = append(data, datatype[:]...)
    l := len(job.Data)
    datalength := gearman.Uint32ToBytes(uint32(l))
    data = append(data, datalength[:]...)
    data = append(data, job.Data...)
    return
}

// Extract the job's result.
func (job *ClientJob) Result() (data []byte, err error) {
    switch job.DataType {
    case gearman.WORK_FAIL:
        job.Handle = string(job.Data)
        err = gearman.ErrWorkFail
        return
    case gearman.WORK_EXCEPTION:
        err = gearman.ErrWorkException
        fallthrough
    case gearman.WORK_COMPLETE:
        s := bytes.SplitN(job.Data, []byte{'\x00'}, 2)
        if len(s) != 2 {
            err = gearman.ErrInvalidData
            return
        }
        job.Handle = string(s[0])
        data = s[1]
    default:
        err = gearman.ErrDataType
    }
    return
}

// Extract the job's update
func (job *ClientJob) Update() (data []byte, err error) {
    if job.DataType != gearman.WORK_DATA && job.DataType != gearman.WORK_WARNING {
        err = gearman.ErrDataType
        return
    }
    s := bytes.SplitN(job.Data, []byte{'\x00'}, 2)
    if len(s) != 2 {
        err = gearman.ErrInvalidData
        return
    }
    if job.DataType == gearman.WORK_WARNING {
        err = gearman.ErrWorkWarning
    }
    job.Handle = string(s[0])
    data = s[1]
    return
}
