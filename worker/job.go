package worker

type Job interface {
	Data() []byte
	SendWarning(data []byte)
	SendData(data []byte)
	UpdateStatus(numerator, denominator int)
}
