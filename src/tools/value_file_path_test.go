package tools

import (
	"testing"
)

func TestComputeValueFilePath(t *testing.T) {
	repositoryPath := "aaa"
	spaceID := uint32(1)
	valueFileID := uint32(2)
	expectedResult := "aaa/1/002/00000002"

	result := ComputeValueFilePath(repositoryPath, spaceID, valueFileID)
	if result != expectedResult {
		t.Fatalf("mismatch: expected '%s' got '%s'", expectedResult, result)
	}
}
