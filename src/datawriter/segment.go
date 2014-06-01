package main

import (
	"datawriter/nodedb"
)

const (
	SegmentStatusActive    = 'A'
	SegmentStatusCancelled = 'C'
	SegmentStatusFinal     = 'F'
	SegmentStatusTombstone = 'T'
)

// NewSegment starts a new segment and returns the  segmentID
func NewSegment(entry SegmentEntry) (uint64, error) {
	stmt := nodedb.Stmts["new-segment"]
	row := stmt.QueryRow(
		entry.CollectionID,
		entry.Key,
		entry.UnifiedID,
		entry.Timestamp,
		entry.SegmentNum,
		entry.ConjoinedPart,
		entry.SourceNodeID,
		entry.HandoffNodeID)
	var segmentID uint64
	err := row.Scan(&segmentID)
	return segmentID, err
}
