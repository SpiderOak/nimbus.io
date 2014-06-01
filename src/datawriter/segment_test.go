package main

import (
	"testing"
	"time"

	"datawriter/nodedb"
)

func TestNewSegment(t *testing.T) {
	if err := nodedb.Initialize(); err != nil {
		t.Fatalf("nodedb.Initialize() %s", err)
	}
	defer nodedb.Close()

	var entry SegmentEntry
	entry.CollectionID = 1
	entry.Key = "test key"
	entry.UnifiedID = 2
	entry.Timestamp = time.Time.UTC(time.Now())
	entry.ConjoinedPart = 0
	entry.SegmentNum = 1
	entry.SourceNodeID = 5
	entry.HandoffNodeID = 0

	var segmentID uint64
	var err error
	if segmentID, err = NewSegment(entry); err != nil {
		t.Fatalf("NewSegment %s", err)
	}
	t.Logf("segment id = %d", segmentID)
}
