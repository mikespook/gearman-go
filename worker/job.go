package worker

type Job interface {
	Err() error
	Data() []byte
	Fn() string
	SendWarning(data []byte)
	SendData(data []byte)
	UpdateStatus(numerator, denominator int)
	Handle() string
	UniqueId() string
}
