package routing

import (
	"testing"
)

func TestConsistentHashDestSingle(t *testing.T) {
	collectionName := "aaa"
	path := "bbb"
	hosts := []string{"aaa"}
	avail := []string{"aaa"}

	result, err := consistentHashDest(hosts, avail, collectionName, path)
	if err != nil {
		t.Fatalf("consistentHashDest %s", err)
	}

	if result != "aaa" {
		t.Fatalf("unexpected result '%s'", result)
	}
}

func TestConsistentHashDestAllAvail(t *testing.T) {
	collectionName := "aaa"
	path := "bbb"
	hosts := []string{"host1", "host2", "host3", "host4", "host5", "host6", "host7", "host8", "host9", "host10"}
	avail := []string{"host1", "host2", "host3", "host4", "host5", "host6", "host7", "host8", "host9", "host10"}

	var expectedResult string
	for n := 0; n < 100; n++ {
		result, err := consistentHashDest(hosts, avail, collectionName, path)
		if err != nil {
			t.Fatalf("%d consistentHashDest %s", n, err)
		}

		if n == 0 {
			expectedResult = result
		} else {
			if result != expectedResult {
				t.Fatalf("%d unexpected result '%s' expecting %s", n, result,
					expectedResult)
			}
		}
	}
}

func TestConsistentHashDestOneAvail(t *testing.T) {
	collectionName := "aaa"
	path := "bbb"
	hosts := []string{"host1", "host2", "host3", "host4", "host5", "host6", "host7", "host8", "host9", "host10"}
	avail := []string{"host3"}

	expectedResult := "host3"
	for n := 0; n < 100; n++ {
		result, err := consistentHashDest(hosts, avail, collectionName, path)
		if err != nil {
			t.Fatalf("%d consistentHashDest %s", n, err)
		}

		if result != expectedResult {
			t.Fatalf("%d unexpected result '%s' expecting %s", n, result,
				expectedResult)
		}
	}
}
