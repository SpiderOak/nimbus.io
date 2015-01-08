package tools

import (
	"testing"
)

func TestFreeMemory(t *testing.T) {
	n, err := FreeMemory()
	if err != nil {
		t.Fatalf("FreeMemory %s", err)
	}

	t.Logf("FreeMemory = %d", n)
}
