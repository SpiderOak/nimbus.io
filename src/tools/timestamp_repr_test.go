package tools

import (
	"testing"
)

const (
	testString = "datetime.datetime(2011, 6, 30, 13, 52, 34, 720271)"
)

func TestParsingtimestampRepr(t *testing.T) {
	timestamp, err := ParseTimestampRepr(testString)
	if err != nil {
		t.Fatalf("ParsetimestampRepr(%s) failed %s", testString, err)
	}
	if timestamp.Year() != 2011 {
		t.Fatalf("invalid year %s", timestamp)
	}
	if timestamp.Month() != 6 {
		t.Fatalf("invalid month %s", timestamp)
	}
	if timestamp.Day() != 30 {
		t.Fatalf("invalid day %s", timestamp)
	}
	if timestamp.Hour() != 13 {
		t.Fatalf("invalid hour %s", timestamp)
	}
	if timestamp.Minute() != 52 {
		t.Fatalf("invalid minute %s", timestamp)
	}
	if timestamp.Second() != 34 {
		t.Fatalf("invalid second %s", timestamp)
	}
}
