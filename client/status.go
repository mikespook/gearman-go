package client

type Status struct {
	Handle                 []byte
	Known, Running         bool
	Numerator, Denominator uint64
}
