// Copyright 2011 - 2012 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package common

import (
    "fmt"
    "bytes"
    "errors"
    "syscall"
)

var (
    ErrJobTimeOut       = errors.New("Do a job time out.")
    ErrInvalidData      = errors.New("Invalid data.")
    ErrWorkWarning      = errors.New("Work warning.")
    ErrWorkFail         = errors.New("Work fail.")
    ErrWorkException    = errors.New("Work exeption.")
    ErrDataType         = errors.New("Invalid data type.")
    ErrOutOfCap         = errors.New("Out of the capability.")
    ErrNotConn          = errors.New("Did not connect to job server.")
    ErrFuncNotFound     = errors.New("The function was not found.")
    ErrConnection       = errors.New("Connection error.")
    ErrNoActiveAgent    = errors.New("No active agent.")
    ErrExecTimeOut      = errors.New("Executing time out.")
    ErrUnknown            = errors.New("Unknown error.")
)
func DisablePanic() {recover()}

// Extract the error message
func GetError(data []byte) (eno syscall.Errno, err error) {
    rel := bytes.SplitN(data, []byte{'\x00'}, 2)
    if len(rel) != 2 {
        err = Errorf("Not a error data: %V", data)
        return
    }
    l := len(rel[0])
    eno = syscall.Errno(BytesToUint32([4]byte{rel[0][l-4], rel[0][l-3], rel[0][l-2], rel[0][l-1]}))
    err = errors.New(string(rel[1]))
    return
}

// Get a formated error
func Errorf(format string, msg ... interface{}) error {
    return errors.New(fmt.Sprintf(format, msg ... ))
}

// An error handler
type ErrorHandler func(error)


