package client

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrWorkWarning = errors.New("Work warning")
	ErrInvalidData = errors.New("Invalid data")
	ErrInvalidId   = errors.New("Invalid ID")
	// ErrWorkFail is returned by Response.Result for a WORK_FAIL: the job
	// failed and the worker sent no payload with it.
	ErrWorkFail = errors.New("Work fail")
	// ErrWorkException is returned by Response.Result for a WORK_EXCEPTION.
	// The worker's exception payload is the data return value; the error only
	// says that the job ended in an exception. Reaching a client at all
	// requires the "exceptions" option, see DefaultExceptions.
	ErrWorkException = errors.New("Work exeption")
	ErrDataType      = errors.New("Invalid data type")
	ErrLostConn      = errors.New("Lost connection with Gearmand")

	// errIncompletePacket tells readLoop to read more before decoding again.
	// It carries no data on purpose: readLoop hits it after every read of a
	// large packet, so building it from the buffer costs time quadratic in
	// the packet size.
	errIncompletePacket = errors.New("Incomplete packet")
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
