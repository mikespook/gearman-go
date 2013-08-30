package worker

import (
	"strconv"
)

type Job interface {
	Data() []byte
	SendWarning(data []byte)
	SendData(data []byte)
	UpdateStatus(numerator, denominator int)
}

type _job struct {
	a *agent
	Handle string
	data []byte
}

func getJob() *_job {
	return &_job{}
}

func (j *_job) Data() []byte {
	return j.data
}

// Send some datas to client.
// Using this in a job's executing.
func (j *_job) SendData(data []byte) {
	req := getRequest()
	req.DataType = WORK_DATA
	hl := len(j.Handle)
	l := hl + len(data) + 1
	req.Data = getBuffer(l)
	copy(req.Data, []byte(j.Handle))
	copy(req.Data[hl + 1:], data)
	j.a.write(req)
}

func (j *_job) SendWarning(data []byte) {
	req := getRequest()
	req.DataType = WORK_WARNING
	hl := len(j.Handle)
	l := hl + len(data) + 1
	req.Data = getBuffer(l)
	copy(req.Data, []byte(j.Handle))
	copy(req.Data[hl + 1:], data)
	j.a.write(req)
}

// Update status.
// Tall client how many percent job has been executed.
func (j *_job) UpdateStatus(numerator, denominator int) {
	n := []byte(strconv.Itoa(numerator))
	d := []byte(strconv.Itoa(denominator))
	req := getRequest()
	req.DataType = WORK_STATUS
	hl := len(j.Handle)
	nl := len(n)
	dl := len(d)
	req.Data = getBuffer(hl + nl + dl + 3)
	copy(req.Data, []byte(j.Handle))
	copy(req.Data[hl+1:], n)
	copy(req.Data[hl+nl+2:], d)
	j.a.write(req)
}

