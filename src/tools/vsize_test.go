package tools

import (
	"testing"
)

func TestVsize(t *testing.T) {
	n, err := GetMyVSize()
	if err != nil {
		t.Fatalf("GetMyVSize %s", err)
	}

	t.Logf("VSize = %d", n)
}
