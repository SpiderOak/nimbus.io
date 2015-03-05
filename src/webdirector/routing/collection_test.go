package routing

import (
	"fmt"
	"testing"
)

type parseCollectionTestEntry struct {
	hostName   string
	collection string
}

var (
	parseCollectionTestData []parseCollectionTestEntry
)

func init() {
	serviceDomain = "servicedomain"
	parseCollectionTestData = []parseCollectionTestEntry{
		parseCollectionTestEntry{"", ""},
		parseCollectionTestEntry{serviceDomain, ""},
		parseCollectionTestEntry{fmt.Sprintf("xxx%s", serviceDomain), ""},
		parseCollectionTestEntry{fmt.Sprintf("aaa.%s", serviceDomain), "aaa"}}
}

func TestParseCollection(t *testing.T) {
	for n, testEntry := range parseCollectionTestData {
		result := parseCollectionFromHostName(testEntry.hostName)
		if result != testEntry.collection {
			t.Fatalf("%d unexpected collection '%s' expecting '%s'", n, result,
				testEntry.collection)
		}
	}
}
