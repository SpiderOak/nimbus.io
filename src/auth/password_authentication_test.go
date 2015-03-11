package auth

import (
	"testing"

	"centraldb"
	//	"types"
)

func TestEmptyPassword(t *testing.T) {
	const customerID uint32 = 42
	var centralDB centraldb.MockCentralDB
	var accessAllowed bool
	var err error

	accessAllowed, err = PasswordIsValid(centralDB, customerID)
	if err != nil {
		t.Fatalf("PasswordIsValid error %s", err)
	}

	if accessAllowed {
		t.Fatalf("expected access denied")
	}
}
