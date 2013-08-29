package client

// Response handler
type ResponseHandler func(*Response)

// Error handler
type ErrorHandler func(error)

// Status handler
// handle, known, running, numerator, denominator
type StatusHandler func(string, bool, bool, uint64, uint64)
