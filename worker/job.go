// Copyright 2011 Xing Xing <mikespook@gmail.com> 
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package worker

import (
    "strconv"
    "bitbucket.org/mikespook/gearman-go/common"
)

// Worker side job
type Job struct {
    Data                []byte
    Handle, UniqueId    string
    agent               *agent
    magicCode, DataType uint32
}

// Create a new job
func newJob(magiccode, datatype uint32, data []byte) (job *Job) {
    return &Job{magicCode: magiccode,
        DataType: datatype,
        Data:     data}
}

// Decode job from byte slice
func decodeJob(data []byte) (job *Job, err error) {
    if len(data) < 12 {
        return nil, common.Errorf("Invalid data: %V", data)
    }
    datatype := common.BytesToUint32([4]byte{data[4], data[5], data[6], data[7]})
    l := common.BytesToUint32([4]byte{data[8], data[9], data[10], data[11]})
    if len(data[12:]) != int(l) {
        return nil, common.Errorf("Invalid data: %V", data)
    }
    data = data[12:]
    job = newJob(common.RES, datatype, data)
    return
}

// Encode a job to byte slice
func (job *Job) Encode() (data []byte) {
    l := len(job.Data)
    tl := l
    if job.Handle != "" {
        tl += len(job.Handle) + 1
    }
    data = make([]byte, 0, tl + 12)

    magiccode := common.Uint32ToBytes(job.magicCode)
    datatype := common.Uint32ToBytes(job.DataType)
    datalength := common.Uint32ToBytes(uint32(tl))

    data = append(data, magiccode[:]...)
    data = append(data, datatype[:]...)
    data = append(data, datalength[:]...)
    if job.Handle != "" {
        data = append(data, []byte(job.Handle)...)
        data = append(data, 0)
    }
    data = append(data, job.Data...)
    return
}

// Send some datas to client.
// Using this in a job's executing.
func (job *Job) UpdateData(data []byte, iswaring bool) {
    result := append([]byte(job.Handle), 0)
    result = append(result, data...)
    var datatype uint32
    if iswaring {
        datatype = common.WORK_WARNING
    } else {
        datatype = common.WORK_DATA
    }
    job.agent.WriteJob(newJob(common.REQ, datatype, result))
}

// Update status.
// Tall client how many percent job has been executed.
func (job *Job) UpdateStatus(numerator, denominator int) {
    n := []byte(strconv.Itoa(numerator))
    d := []byte(strconv.Itoa(denominator))
    result := append([]byte(job.Handle), 0)
    result = append(result, n...)
    result = append(result, d...)
    job.agent.WriteJob(newJob(common.REQ, common.WORK_STATUS, result))
}
