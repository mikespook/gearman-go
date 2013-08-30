package client

// Status handler
// handle, known, running, numerator, denominator
type StatusHandler func(string, bool, bool, uint64, uint64)

type Status struct {
	Handle                 string
	Known, Running         bool
	Numerator, Denominator uint64
}
