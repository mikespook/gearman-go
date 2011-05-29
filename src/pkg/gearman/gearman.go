// Copyright 2011 Xing Xing <mikespook@gmail.com> All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/*
This module is Gearman API for golang. 
The protocol was implemented by native way.
*/

package gearman

import (
    "bytes"
    "os"
)

const (
    // tcp4 is tested. You can modify this to 'tcp' for both ipv4 and ipv6,
    // or 'tcp6' only for ipv6.
    TCP = "tcp4"
    // the number limited for job servers.
    WORKER_SERVER_CAP = 32
    // the number limited for functions.
    WORKER_FUNCTION_CAP = 512
    // queue size
    QUEUE_CAP = 512
    // read buffer size
    BUFFER_SIZE = 1024


    // \x00REQ
    REQ     = 5391697
    REQ_STR = "\x00REQ"
    // \x00RES
    RES     = 5391699
    RES_STR = "\x00RES"

    // package data type
    CAN_DO          = 1
    CANT_DO         = 2
    RESET_ABILITIES = 3
    PRE_SLEEP       = 4
    NOOP            = 6
    JOB_CREATED     = 8
    GRAB_JOB        = 9
    NO_JOB          = 10
    JOB_ASSIGN      = 11
    WORK_STATUS     = 12
    WORK_COMPLETE   = 13
    WORK_FAIL       = 14
    GET_STATUS      = 15
    ECHO_REQ        = 16
    ECHO_RES        = 17
    ERROR           = 19
    STATUS_RES      = 20
    SET_CLIENT_ID   = 22
    CAN_DO_TIMEOUT  = 23
    WORK_EXCEPTION  = 25
    WORK_DATA       = 28
    WORK_WARNING    = 29
    GRAB_JOB_UNIQ   = 30
    JOB_ASSIGN_UNIQ = 31

    SUBMIT_JOB         = 7
    SUBMIT_JOB_BG      = 18
    SUBMIT_JOB_HIGH    = 21
    SUBMIT_JOB_HIGH_BG = 32
    SUBMIT_JOB_LOW     = 33
    SUBMIT_JOB_LOW_BG  = 34

    // Job type
    // JOB_NORMAL | JOB_BG means a normal level job run in background
    // normal level
    JOB_NORMAL = 0
    // background job
    JOB_BG = 1
    // low level
    JOB_LOW = 2
    // high level
    JOB_HIGH = 4
)

// No use
type Job interface {
    Encode() []byte
}

// Extract the error message
func getError(data []byte) (eno os.Errno, err os.Error) {
    rel := bytes.Split(data, []byte{'\x00'}, 2)
    if len(rel) != 2 {
        err = os.NewError("The input is not a error data.")
        return
    }
    l := len(rel[0])
    eno = os.Errno(byteToUint32([4]byte{rel[0][l-4], rel[0][l-3], rel[0][l-2], rel[0][l-1]}))
    err = os.NewError(string(rel[1]))
    return
}

// Decode [4]byte to uint32 
func byteToUint32(buf [4]byte) uint32 {
    return uint32(buf[0])<<24 +
        uint32(buf[1])<<16 +
        uint32(buf[2])<<8 +
        uint32(buf[3])
}

// Encode uint32 to [4]byte
func uint32ToByte(i uint32) (data [4]byte) {
    data[0] = byte((i >> 24) & 0xff)
    data[1] = byte((i >> 16) & 0xff)
    data[2] = byte((i >> 8) & 0xff)
    data[3] = byte(i & 0xff)
    return
}
