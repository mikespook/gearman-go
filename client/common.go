// Copyright 2011 - 2012 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

const (
	NETWORK = "tcp"
	// queue size
	QUEUE_SIZE = 8
	// read buffer size
	BUFFER_SIZE = 1024
	// min packet length
	MIN_PACKET_LEN = 12

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
)

const (
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

func getBuffer(l int) (buf []byte) {
	// TODO add byte buffer pool
	buf = make([]byte, l)
	return
}
