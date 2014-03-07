package worker

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrNoneAgents = errors.New("None active agents")
	ErrNoneFuncs  = errors.New("None functions")
	ErrTimeOut    = errors.New("Executing time out")
	ErrUnknown    = errors.New("Unknown error")
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

// An error handler
type ErrorHandler func(error)
