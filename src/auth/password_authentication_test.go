package auth

import (
	"testing"

	"centraldb"
	"types"
)

type testAuthEntry struct {
	customerID    uint32
	userName      string
	method        string
	timestamp     string
	path          string
	customerKeyID uint32
	key           []byte
	authText      string
	expectError   bool
}

func TestPasswordAuthentication(t *testing.T) {
	// test data generated from python library lumberyard
	testEntries := []testAuthEntry{
		testAuthEntry{
			customerID:    42,
			userName:      "motoboto-benchmark-000",
			method:        "HEAD",
			timestamp:     "1344970933",
			path:          "/data/xxx",
			customerKeyID: 2,
			key:           []byte("iKn/OxpggHSXzB0oUAihTMTf+n6Bsyywwm3bXMQfdKo"),
			authText:      "NIMBUS.IO 2:c4d946d1089f11d310ae8bd8f52501212a3eb50cc800b0688d3df7bf65f04ce7",
			expectError:   false,
		},
		testAuthEntry{
			customerID:    42,
			userName:      "motoboto-benchmark-003",
			method:        "POST",
			timestamp:     "1344970936",
			path:          "/data/xxx",
			customerKeyID: 5,
			key:           []byte("9dJwBh1YWNlrs31F+Fx2BY2KZUtFYmBH3hmocH+6Ggk"),
			authText:      "NIMBUS.IO 5:015c53c88a0531ef1f998fcfee01612627de2c9de8cbaa76d1c99b0de0f32d6d",
			expectError:   false,
		},
		testAuthEntry{
			customerID:    42,
			userName:      "motoboto-benchmark-003",
			method:        "POST",
			timestamp:     "1344970936",
			path:          "/data/xxx",
			customerKeyID: 5,
			key:           []byte("9dJwBh1YWNlrs31F+Fx2BY2KZUtFYmBH3hmocH+6Ggk"),
			authText:      "NIMBUS.IO 5:015c53c98a0531ef1f998fcfee01612627de2c9de8cbaa76d1c99b0de0f32d6d",
			expectError:   true,
		},
	}
	var centralDB centraldb.MockCentralDB
	var err error

	for i, entry := range testEntries {
		centralDB.GetCustomerRowByIDFunc = func(_ uint32) (types.CustomerRow, error) {
			return types.CustomerRow{UserName: entry.userName}, nil
		}
		centralDB.GetCustomerKeyRowFunc = func(_ uint32) (types.CustomerKeyRow, error) {
			return types.CustomerKeyRow{ID: entry.customerKeyID,
				CustomerID: entry.customerID, Key: entry.key}, nil
		}

		err = PasswordAuthentication(centralDB,
			entry.customerID,
			entry.method,
			entry.timestamp,
			entry.path,
			entry.authText)

		if !entry.expectError && err != nil {
			t.Fatalf("#%d unexpected error %s", i+1, err)
		}

		if entry.expectError && err == nil {
			t.Fatalf("#%d expected error", i+1)
		}
	}
}
