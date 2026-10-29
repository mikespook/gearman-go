package client

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

// readLoop retries decodeResponse after every read until a packet is whole, so
// a large response is decoded hundreds of times while it is still incomplete.
// Each of those failures must stay cheap: building the error from the buffer
// made receiving a response quadratic in its size.
func TestDecodeResponseIncompleteIsCheap(t *testing.T) {
	const size = 4 << 20
	pkt := resPacket(dtWorkComplete, append([]byte("H:x:1\x00"), make([]byte, size)...))

	for _, n := range []int{minPacketLength - 1, minPacketLength, len(pkt) / 2, len(pkt) - 1} {
		_, l, err := decodeResponse(pkt[:n])
		if !errors.Is(err, errIncompletePacket) {
			t.Fatalf("decodeResponse(%d of %d bytes) err = %v, want errIncompletePacket", n, len(pkt), err)
		}
		if l != 0 {
			t.Errorf("decodeResponse(%d of %d bytes) consumed %d bytes, want 0", n, len(pkt), l)
		}
		if len(err.Error()) > 64 {
			t.Errorf("decodeResponse(%d of %d bytes) error is %d bytes long; it must not carry the buffer", n, len(pkt), len(err.Error()))
		}
	}
}

func TestLargeResponseArrivesWhole(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()

	const handle = "H:localhost:10"
	payload := bytes.Repeat([]byte("0123456789abcdef"), (8<<20)/16)
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		switch dataType {
		case dtOptionReq:
			conn.Write(resPacket(dtOptionRes, data))
		case dtSubmitJob:
			conn.Write(resPacket(dtJobCreated, []byte(handle)))
			conn.Write(resPacket(dtWorkComplete, append([]byte(handle+"\x00"), payload...)))
		}
		return true
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.ErrorHandler = func(error) {}

	got := make(chan []byte, 1)
	if _, err := c.Do("Large", []byte("x"), JobNormal, func(r *Response) {
		if data, err := r.Result(); err == nil {
			got <- data
		}
	}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	// Linear reassembly takes milliseconds; the quadratic one took minutes
	// for this size. The bound only separates the two.
	select {
	case data := <-got:
		if !bytes.Equal(data, payload) {
			t.Fatalf("payload differs: got %d bytes, want %d", len(data), len(payload))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("an 8 MB WORK_COMPLETE did not arrive within 10s")
	}
}
