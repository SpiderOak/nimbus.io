package mgmtapi

import (
	"os"
	"strings"
	"testing"
)

func TestEmptyManagementAPI(t *testing.T) {
	if err := os.Setenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST", ""); err != nil {
		t.Fatalf("os.Setenv failed: %s", err)
	}

	_, err := NewManagementAPIDestinations()
	if err == nil {
		t.Fatalf("expecting error on empty NIMBUSIO_MANAGEMENT_API_REQUEST_DEST")
	}
}

func TestSingleManagementAPI(t *testing.T) {
	testHost := "host1"

	if err := os.Setenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST", testHost); err != nil {
		t.Fatalf("os.Setenv failed: %s", err)
	}

	d, err := NewManagementAPIDestinations()
	if err != nil {
		t.Fatalf("NewManagementAPIDestinations %s", err)
	}

	if d.Peek() != testHost {
		t.Fatalf("unexpected Peek() %s", d.Peek())
	}

	for i := 0; i < 100; i++ {
		if d.Next() != testHost {
			t.Fatalf("unexpected result %s", d.Next())
		}
	}
}

func TestMultipleManagementAPI(t *testing.T) {
	testHosts := []string{"host1", "host2", "host3"}
	hostString := strings.Join(testHosts, " ")

	if err := os.Setenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST", hostString); err != nil {
		t.Fatalf("os.Setenv failed: %s", err)
	}

	d, err := NewManagementAPIDestinations()
	if err != nil {
		t.Fatalf("NewManagementAPIDestinations %s", err)
	}

	if d.Peek() != testHosts[0] {
		t.Fatalf("unexpected Peek() %s", d.Peek())
	}

	for i := 0; i < 100; i++ {
		if d.Next() != testHosts[i%len(testHosts)] {
			t.Fatalf("unexpected result %s", d.Next())
		}
	}
}
