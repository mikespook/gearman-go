package client

import (
	"testing"
)

func TestAutoInc(t *testing.T) {
	ai := NewAutoIncId()

	previous := ai.Id("testfuncname", "fakepayload")
	for i := 0; i < 10; i++ {
		id := ai.Id("testfuncname", "fakepayload2")
		if id == previous {
			t.Errorf("Id not unique, previous and current %s", id)
		}
		previous = id
	}
}
