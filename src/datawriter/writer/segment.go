package writer

import (
	"datawriter/nodedb"
	"datawriter/types"
)

const (
	SegmentStatusActive    = 'A'
	SegmentStatusCancelled = 'C'
	SegmentStatusFinal     = 'F'
	SegmentStatusTombstone = 'T'
)

// NewSegment starts a new segment and returns the  segmentID
func NewSegment(entry types.SegmentEntry) (uint64, error) {
	var segmentID uint64
	var err error

	if entry.HandoffNodeID > 0 {
		stmt := nodedb.Stmts["new-segment-for-handoff"]
		row := stmt.QueryRow(
			entry.CollectionID,
			entry.Key,
			entry.UnifiedID,
			entry.Timestamp,
			entry.SegmentNum,
			entry.ConjoinedPart,
			entry.SourceNodeID,
			entry.HandoffNodeID)
		err = row.Scan(&segmentID)
	} else {
		stmt := nodedb.Stmts["new-segment"]
		row := stmt.QueryRow(
			entry.CollectionID,
			entry.Key,
			entry.UnifiedID,
			entry.Timestamp,
			entry.SegmentNum,
			entry.ConjoinedPart,
			entry.SourceNodeID)
		err = row.Scan(&segmentID)
	}

	return segmentID, err
}
