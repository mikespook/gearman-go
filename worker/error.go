package worker

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrJobTimeOut    = errors.New("Do a job time out")
	ErrInvalidData   = errors.New("Invalid data")
	ErrWorkWarning   = errors.New("Work warning")
	ErrWorkFail      = errors.New("Work fail")
	ErrWorkException = errors.New("Work exeption")
	ErrDataType      = errors.New("Invalid data type")
	ErrOutOfCap      = errors.New("Out of the capability")
	ErrNotConn       = errors.New("Did not connect to job server")
	ErrFuncNotFound  = errors.New("The function was not found")
	ErrConnection    = errors.New("Connection error")
	ErrNoActiveAgent = errors.New("No active agent")
	ErrTimeOut       = errors.New("Executing time out")
	ErrUnknown       = errors.New("Unknown error")
	ErrConnClosed    = errors.New("Connection closed")
)

// Extract the error message
func GetError(data []byte) (err error) {
	rel := bytes.SplitN(data, []byte{'\x00'}, 2)
	if len(rel) != 2 {
		err = fmt.Errorf("Not a error data: %V", data)
		return
	}
	err = errors.New(fmt.Sprintf("%s: %s", rel[0], rel[1]))
	return
}

// An error handler
type ErrorHandler func(error)
