package writer

import (
	"bytes"
	"testing"

	"tools"

	"datawriter/nodedb"
)

func TestOutputValueFile(t *testing.T) {
	if err := nodedb.Initialize(); err != nil {
		t.Fatalf("nodedb.Initialize() %s", err)
	}

	info, err := tools.NewFileSpaceInfo(nodedb.NodeDB)
	if err != nil {
		t.Fatalf("NewFileSpaceInfo( %s", err)
	}

	valueFile, err := NewOutputValueFile(info)
	if err != nil {
		t.Fatalf("NewOutputValueFile %s", err)
	}

	collectionID := uint32(42)
	segmentID := uint64(88)
	dataSize := 1024
	data := bytes.Repeat([]byte{'a'}, dataSize)

	if _, err := valueFile.Store(collectionID, segmentID, data); err != nil {
		t.Fatalf("valueFile.Store %s", err)
	}

	if valueFile.Size() != uint64(dataSize) {
		t.Fatalf("size mismatch: expecting %d, found %d", dataSize,
			valueFile.Size())
	}

	if err := valueFile.Close(); err != nil {
		t.Fatalf("valueFile.Close() %s", err)
	}
}
