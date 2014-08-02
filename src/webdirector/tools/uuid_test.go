package tools

import (
	"testing"
)

func TestCreateUUID(t *testing.T) {
	testCount := 100

	// expecting a string of the form 5b31a550-3a95-4178-ad9c-992a819d693b
	expectedLength := 36

	uuidSet := make(map[string]struct{}, testCount)

	for i := 0; i < testCount; i++ {
		uuid, err := CreateUUID()
		if err != nil {
			t.Fatalf("%d CreateUUID error %s", i+1, err)
		}

		if len(uuid) != expectedLength {
			t.Fatalf("%d invalid UUID '%s'", i+1, uuid)
		}

		if _, ok := uuidSet[uuid]; ok {
			t.Fatalf("%d duplicate UUID %s", i+1, uuid)
		}

		uuidSet[uuid] = struct{}{}
	}
}
