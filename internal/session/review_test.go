package session

import (
	"github.com/eidonix-ai/cletus-connector/protocol"
	"testing"
)

func TestInstanceIDIsStableWithinProcess(t *testing.T) {
	first, err := processInstance()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		next, err := processInstance()
		if err != nil || first != next {
			t.Fatal("instance changed within process")
		}
	}
	other, err := protocol.NewInstanceID()
	if err != nil || first == other {
		t.Fatal("instance generation not random")
	}
}
