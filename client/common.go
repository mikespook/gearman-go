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
	CAN_DO          = 0x1
	CANT_DO         = 0x2
	RESET_ABILITIES = 0x3
	PRE_SLEEP       = 0x4
	NOOP            = 0x6
	JOB_CREATED     = 0x8
	GRAB_JOB        = 0x9
	NO_JOB          = 0xa
	JOB_ASSIGN      = 0xb
	WORK_STATUS     = 0xc
	WORK_COMPLETE   = 0xd
	WORK_FAIL       = 0xe
	GET_STATUS      = 0xf
	ECHO_REQ        = 0x10
	ECHO_RES        = 0x11
	ERROR           = 0x13
	STATUS_RES      = 0x14
	SET_CLIENT_ID   = 0x16
	CAN_DO_TIMEOUT  = 0x17
	WORK_EXCEPTION  = 0x19
	WORK_DATA       = 0x1c
	WORK_WARNING    = 0x1d
	GRAB_JOB_UNIQ   = 0x1e
	JOB_ASSIGN_UNIQ = 0x1f

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
