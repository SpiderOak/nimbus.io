package writer

import (
	"testing"

	"tools"
)

func TestOutputValueFile(t *testing.T) {
	sqlDB, err := tools.OpenLocalNodeDatabase()
	if err != nil {
		t.Fatalf("OpenLocalNodeDatabase() %s", err)
	}
	defer sqlDB.Close()

	_, err = NewOutputValueFile(sqlDB)
	if err != nil {
		t.Fatalf("NewOutputValueFile %s", err)
	}
}
