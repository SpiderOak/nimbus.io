package tools

import (
	"testing"
)

func TestHostResolver(t *testing.T) {
	resolver := NewHostResolver()

	_, err := resolver.Lookup("$$$")
	if err == nil {
		t.Fatalf("expecting error")
	}

	address, err := resolver.Lookup("localhost")
	if err != nil {
		t.Fatalf("lookup error %s", err)
	}

	t.Logf("address = %s", address)

	// this one should come from the cache
	address, err = resolver.Lookup("localhost")
	if err != nil {
		t.Fatalf("lookup error %s", err)
	}

	t.Logf("address = %s", address)
}
