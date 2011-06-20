package gearman

import (
    "testing"
)

func TestByteToUint32(t * testing.T) {
    if u := byteToUint32([4]byte{0, 0, 0, 0}); u != 0 {
        t.Errorf("Number %t should be zero.", u)
    }
    if u := byteToUint32([4]byte{0xF, 0xF, 0xF, 0xF}); u != 252645135 {
        t.Errorf("Number %t should be 252645135.", u)
    }
}

func BenchmarkByteToUnit32(b * testing.B) {
    for i := 0; i < b.N; i++ {
        byteToUint32([4]byte{0xF, 0xF, 0xF, 0xF});
    }
}

func TestUint32ToByte(t * testing.T) {
    if b := uint32ToByte(0); len(b) != 4 {
        t.Errorf("%t", b)
    } else {
        for i := 0; i < 4; i ++ {
            if b[i] != 0 {
                t.Errorf("%t", b[i])
            }
        }
    }
    if b := uint32ToByte(252645135); len(b) != 4 {
        t.Errorf("%t", b)
    } else {
        for i := 0; i < 4; i++ {
            if b[i] != 0xf {
                t.Errorf("%t", b[i])
            }
        }
    }
}

func BenchmarkUint32ToByte(b *testing.B) {
    for i := 0; i < b.N; i++ {
        uint32ToByte(123456);
    }
}
