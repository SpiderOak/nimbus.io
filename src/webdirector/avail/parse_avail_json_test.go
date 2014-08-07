package avail

import (
	"testing"
)

func TestParseAvailJSON(t *testing.T) {
	testJSON := []byte("{\n    \"reachable\": true, \n    \"timestamp\": 1407355954.218035\n}")

	hostAvail, err := parseAvailJSON(testJSON)
	if err != nil {
		t.Fatalf("parse failed %s", err)
	}

	if !hostAvail.Reachable {
		t.Fatalf("expected reachable = true, %q", hostAvail)
	}
}
