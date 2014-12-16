package tools

import (
	"os"
	"testing"
)

func TestFileSpaceInfo(t *testing.T) {

	sqlDB, err := OpenLocalNodeDatabase()
	if err != nil {
		t.Fatalf("OpenLocalNodeDatabase() %s", err)
	}
	defer sqlDB.Close()

	info, err := NewFileSpaceInfo(sqlDB)
	if err != nil {
		t.Fatalf("NewFileSpaceInfo(sqlDB) %s", err)
	}

	repositoryPath := os.Getenv("NIMBUSIO_REPOSITORY_PATH")
	if err := info.SanityCheck(repositoryPath); err != nil {
		t.Fatalf("SanityCheck(%s) %s", repositoryPath, err)
	}

	for _, purpose := range FileSpacePurpose {
		spaceID, space, err := info.FindMaxAvailSpaceID(purpose)
		if err != nil {
			t.Fatalf("FindMaxAvailSpaceID(%s) %s", purpose, err)
		}
		t.Logf("FindMaxAvailSpaceID(%s) = %d, %d", purpose, spaceID, space)
	}

}
