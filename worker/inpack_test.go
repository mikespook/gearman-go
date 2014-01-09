package worker

import (
	"bytes"
	"testing"
)

var (
	inpackcases = map[uint32]map[string]string{
		dtNoop: map[string]string{
			"src": "\x00RES\x00\x00\x00\x06\x00\x00\x00\x00",
		},
		dtNoJob: map[string]string{
			"src": "\x00RES\x00\x00\x00\x0a\x00\x00\x00\x00",
		},
		dtJobAssign: map[string]string{
			"src":    "\x00RES\x00\x00\x00\x0b\x00\x00\x00\x07a\x00b\x00xyz",
			"handle": "a",
			"fn":     "b",
			"data":   "xyz",
		},
		dtJobAssignUniq: map[string]string{
			"src":    "\x00RES\x00\x00\x00\x1F\x00\x00\x00\x09a\x00b\x00c\x00xyz",
			"handle": "a",
			"fn":     "b",
			"uid":    "c",
			"data":   "xyz",
		},
	}
)

func TestInPack(t *testing.T) {
	for k, v := range inpackcases {
		inpack, _, err := decodeInPack([]byte(v["src"]))
		if err != nil {
			t.Error(err)
		}
		if inpack.dataType != k {
			t.Errorf("DataType: %d expected, %d got.", k, inpack.dataType)
		}
		if handle, ok := v["handle"]; ok {
			if inpack.handle != handle {
				t.Errorf("Handle: %s expected, %s got.", handle, inpack.handle)
			}
		}
		if fn, ok := v["fn"]; ok {
			if inpack.fn != fn {
				t.Errorf("FuncName: %s expected, %s got.", fn, inpack.fn)
			}
		}
		if uid, ok := v["uid"]; ok {
			if inpack.uniqueId != uid {
				t.Errorf("UID: %s expected, %s got.", uid, inpack.uniqueId)
			}
		}
		if data, ok := v["data"]; ok {
			if bytes.Compare([]byte(data), inpack.data) != 0 {
				t.Errorf("UID: %v expected, %v got.", data, inpack.data)
			}
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, v := range inpackcases {
			_, _, err := decodeInPack([]byte(v["src"]))
			if err != nil {
				b.Error(err)
			}
		}
	}
}
