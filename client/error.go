package client

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrWorkWarning   = errors.New("Work warning")
	ErrInvalidData   = errors.New("Invalid data")
	ErrWorkFail      = errors.New("Work fail")
	ErrWorkException = errors.New("Work exeption")
	ErrDataType      = errors.New("Invalid data type")
	ErrLostConn      = errors.New("Lost connection with Gearmand")
)

// Extract the error message
func getError(data []byte) (err error) {
	rel := bytes.SplitN(data, []byte{'\x00'}, 2)
	if len(rel) != 2 {
		err = fmt.Errorf("Not a error data: %v", data)
		return
	}
	err = fmt.Errorf("%s: %s", rel[0], rel[1])
	return
}

// Error handler
type ErrorHandler func(error)
