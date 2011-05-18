package gearman

const (
    TCP = "tcp4"
    WORKER_SERVER_CAP = 32
    WORKER_FUNCTION_CAP = 512


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
)


