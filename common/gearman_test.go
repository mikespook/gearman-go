package common

import (
    "bytes"
    "testing"
)

var (
    testCase = map[uint32][4]byte {
        0: [...]byte{0, 0, 0, 0},
        1: [...]byte{0, 0, 0, 1},
        256: [...]byte{0, 0, 1, 0},
        256 * 256: [...]byte{0, 1, 0, 0},
        256 * 256 * 256: [...]byte{1, 0, 0, 0},
        256 * 256 * 256 + 256 * 256 + 256 + 1: [...]byte{1, 1, 1, 1},
        4294967295 : [...]byte{0xFF, 0xFF, 0xFF, 0xFF},
    }
)

func TestUint32ToBytes(t *testing.T) {
    for k, v := range testCase {
        b := Uint32ToBytes(k)
        if bytes.Compare(b[:], v[:]) != 0 {
            t.Errorf("%v was expected, but %v was got", v, b)
        }
    }
}

func TestBytesToUint32s(t *testing.T) {
    for k, v := range testCase {
        u := BytesToUint32([4]byte(v))
        if u != k {
            t.Errorf("%v was expected, but %v was got", k, u)
        }
    }
}

func BenchmarkByteToUnit32(b * testing.B) {
    for i := 0; i < b.N; i++ {
        BytesToUint32([4]byte{0xF, 0xF, 0xF, 0xF});
    }
}

func BenchmarkUint32ToByte(b *testing.B) {
    for i := 0; i < b.N; i++ {
        Uint32ToBytes(123456);
    }
}
