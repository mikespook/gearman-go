package client

type Status struct {
	Handle                 string
	Known, Running         bool
	Numerator, Denominator uint64
}
