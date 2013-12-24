package worker

import (
	"bytes"
	"testing"
)

var (
	outpackcases = map[uint32]map[string]string{
		CAN_DO: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x01\x00\x00\x00\x01a",
			"data": "a",
		},
		CAN_DO_TIMEOUT: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x17\x00\x00\x00\x06a\x00\x00\x00\x00\x01",
			"data": "a\x00\x00\x00\x00\x01",
		},
		CANT_DO: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x02\x00\x00\x00\x01a",
			"data": "a",
		},
		RESET_ABILITIES: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x03\x00\x00\x00\x00",
		},
		PRE_SLEEP: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x04\x00\x00\x00\x00",
		},
		GRAB_JOB: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x09\x00\x00\x00\x00",
		},
		GRAB_JOB_UNIQ: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x1E\x00\x00\x00\x00",
		},
		WORK_DATA: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x1C\x00\x00\x00\x03a\x00b",
			"data": "a\x00b",
		},
		WORK_WARNING: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x1D\x00\x00\x00\x03a\x00b",
			"data": "a\x00b",
		},
		WORK_STATUS: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x0C\x00\x00\x00\x08a\x0050\x00100",
			"data": "a\x0050\x00100",
		},
		WORK_COMPLETE: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x0D\x00\x00\x00\x03a\x00b",
			"data": "a\x00b",
		},
		WORK_FAIL: map[string]string{
			"src":    "\x00REQ\x00\x00\x00\x0E\x00\x00\x00\x01a",
			"handle": "a",
		},
		WORK_EXCEPTION: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x19\x00\x00\x00\x03a\x00b",
			"data": "a\x00b",
		},
		SET_CLIENT_ID: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x16\x00\x00\x00\x01a",
			"data": "a",
		},
		ALL_YOURS: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x18\x00\x00\x00\x00",
		},
	}
)

func TestOutPack(t *testing.T) {
	for k, v := range outpackcases {
		outpack := getOutPack()
		outpack.dataType = k
		if handle, ok := v["handle"]; ok {
			outpack.handle = handle
		}
		if data, ok := v["data"]; ok {
			outpack.data = []byte(data)
		}
		data := outpack.Encode()
		if bytes.Compare([]byte(v["src"]), data) != 0 {
			t.Errorf("%d: %X expected, %X got.", k, v["src"], data)
		}
	}
}
