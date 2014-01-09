package worker

import (
	"bytes"
	"testing"
)

var (
	outpackcases = map[uint32]map[string]string{
		dtCanDo: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x01\x00\x00\x00\x01a",
			"data": "a",
		},
		dtCanDoTimeout: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x17\x00\x00\x00\x06a\x00\x00\x00\x00\x01",
			"data": "a\x00\x00\x00\x00\x01",
		},
		dtCantDo: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x02\x00\x00\x00\x01a",
			"data": "a",
		},
		dtResetAbilities: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x03\x00\x00\x00\x00",
		},
		dtPreSleep: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x04\x00\x00\x00\x00",
		},
		dtGrabJob: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x09\x00\x00\x00\x00",
		},
		dtGrabJobUniq: map[string]string{
			"src": "\x00REQ\x00\x00\x00\x1E\x00\x00\x00\x00",
		},
		dtWorkData: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x1C\x00\x00\x00\x03a\x00b",
			"data": "a\x00b",
		},
		dtWorkWarning: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x1D\x00\x00\x00\x03a\x00b",
			"data": "a\x00b",
		},
		dtWorkStatus: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x0C\x00\x00\x00\x08a\x0050\x00100",
			"data": "a\x0050\x00100",
		},
		dtWorkComplete: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x0D\x00\x00\x00\x03a\x00b",
			"data": "a\x00b",
		},
		dtWorkFail: map[string]string{
			"src":    "\x00REQ\x00\x00\x00\x0E\x00\x00\x00\x01a",
			"handle": "a",
		},
		dtWorkException: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x19\x00\x00\x00\x03a\x00b",
			"data": "a\x00b",
		},
		dtSetClientId: map[string]string{
			"src":  "\x00REQ\x00\x00\x00\x16\x00\x00\x00\x01a",
			"data": "a",
		},
		dtAllYours: map[string]string{
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

func BenchmarkEncode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for k, v := range outpackcases {
			outpack := getOutPack()
			outpack.dataType = k
			if handle, ok := v["handle"]; ok {
				outpack.handle = handle
			}
			if data, ok := v["data"]; ok {
				outpack.data = []byte(data)
			}
			outpack.Encode()
		}
	}
}
