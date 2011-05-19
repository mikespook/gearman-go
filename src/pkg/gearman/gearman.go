package gearman

import (
    "os"
)

const (
    TCP = "tcp4"
    WORKER_SERVER_CAP = 32
    WORKER_FUNCTION_CAP = 512
    QUEUE_CAP = 512


     // \x00REQ
    REQ = 5391697
    // \x00RES
    RES = 5391699

    CAN_DO = 1
    CANT_DO = 2
    RESET_ABILITIES = 3
    PRE_SLEEP = 4
    NOOP = 6
    GRAB_JOB = 9
    NO_JOB = 10
    JOB_ASSIGN = 11
    WORK_STATUS = 12
    WORK_COMPLETE = 13
    WORK_FAIL = 14
    ECHO_REQ = 16
    ECHO_RES = 17
    ERROR = 19
    SET_CLIENT_ID = 22
    CAN_DO_TIMEOUT = 23
    WORK_EXCEPTION = 25
    WORK_DATA = 28
    WORK_WARNING = 29
    GRAB_JOB_UNIQ = 30
    JOB_ASSIGN_UNIQ = 31

    SUBMIT_JOB = 7
    SUBMIT_JOB_BG = 18
    SUBMIT_JOB_HIGH = 21
    SUBMIT_JOB_HIGH_BG = 32
    SUBMIT_JOB_LOW = 33
    SUBMIT_JOB_LOW_BG = 34

    JOB_NORMAL = 0
    JOB_BG = 1
    JOB_LOW = 2
    JOB_HIGH = 4
)

type Job interface {
    Encode() []byte
}

func splitByteArray(slice []byte, spot byte) (data [][]byte){
    data = make([][]byte, 0, 10)
    start, end := 0, 0
    for i, v := range slice {
        if v == spot {
            if start != end {
                data = append(data, slice[start:end])
            }
            start, end = i + 1, i + 1
        } else {
            end ++
        }
    }
    data = append(data, slice[start:])
    return
}

func getError(data []byte) (eno os.Errno, err os.Error) {
    rel := splitByteArray(data, '\x00')
    if len(rel) != 2 {
        err = os.NewError("The input is not a error data.")
        return
    }
    l := len(rel[0])
    eno = os.Errno(byteToUint32([4]byte{rel[0][l-4], rel[0][l-3], rel[0][l-2], rel[0][l - 1]}))
    err = os.NewError(string(rel[1]))
    return
}

func byteToUint32(buf [4]byte) uint32 {
    return uint32(buf[0]) << 24 +
        uint32(buf[1]) << 16 +
        uint32(buf[2]) << 8 +
        uint32(buf[3])
}

func uint32ToByte(i uint32) (data [4]byte) {
    data[0] = byte((i >> 24) & 0xff)
    data[1] = byte((i >> 16) & 0xff)
    data[2] = byte((i >> 8) & 0xff)
    data[3] = byte(i & 0xff)
    return
}
