package worker

const (
	Network = "tcp"
	// queue size
	queueSize int = 8
	// read buffer size
	bufferSize = 1024
	// min packet length
	minPacketLength = 12

	// \x00REQ
	req    = 5391697
	reqStr = "\x00REQ"
	// \x00RES
	res    = 5391699
	resStr = "\x00RES"

	// package data type
	dtCanDo          = 1
	dtCantDo         = 2
	dtResetAbilities = 3
	dtPreSleep       = 4
	dtNoop           = 6
	dtJobCreated     = 8
	dtGrabJob        = 9
	dtNoJob          = 10
	dtJobAssign      = 11
	dtWorkStatus     = 12
	dtWorkComplete   = 13
	dtWorkFail       = 14
	dtGetStatus      = 15
	dtEchoReq        = 16
	dtEchoRes        = 17
	dtError          = 19
	dtStatusRes      = 20
	dtSetClientId    = 22
	dtCanDoTimeout   = 23
	dtAllYours       = 24
	dtWorkException  = 25
	dtWorkData       = 28
	dtWorkWarning    = 29
	dtGrabJobUniq    = 30
	dtJobAssignUniq  = 31
)

func getBuffer(l int) (buf []byte) {
	// TODO add byte buffer pool
	buf = make([]byte, l)
	return
}
