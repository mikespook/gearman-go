package worker

type Job interface {
	Err() error
	Data() []byte
	SendWarning(data []byte)
	SendData(data []byte)
	UpdateStatus(numerator, denominator int)
}
