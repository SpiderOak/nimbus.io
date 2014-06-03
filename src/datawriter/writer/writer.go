package writer

import (
	"fmt"
	"time"

	"tools"

	"datawriter/nodedb"
	"datawriter/types"
)

type NimbusioWriter interface {

	// StartSegment initializes a new segment and prepares to receive data
	// for it
	StartSegment(segmentEntry types.SegmentEntry) error

	// StoreSequence stores data for  an initialized segment
	StoreSequence(segmentEntry types.SegmentEntry,
		sequenceEntry types.SequenceEntry, data []byte) error
}

type segmentKey struct {
	UnifiedID     uint64
	ConjoinedPart uint32
	SegmentNum    uint8
}

type segmentMapEntry struct {
	SegmentID      uint64
	LastActionTime time.Time
}

// map data contained in messages onto our internal segment id
type nimbusioWriter struct {
	SegmentMap    map[segmentKey]segmentMapEntry
	FileSpaceInfo tools.FileSpaceInfo
	ValueFile     OutputValueFile
}

// NewNimbusioWriter returns an entity that implements the NimbusioWriter interface
func NewNimbusioWriter() (NimbusioWriter, error) {
	var err error
	var writer nimbusioWriter
	writer.SegmentMap = make(map[segmentKey]segmentMapEntry)

	if writer.FileSpaceInfo, err = tools.NewFileSpaceInfo(nodedb.NodeDB); err != nil {
		return nil, err
	}

	if writer.ValueFile, err = NewOutputValueFile(writer.FileSpaceInfo); err != nil {
		return nil, err
	}

	return &writer, nil
}

func (writer *nimbusioWriter) StartSegment(segmentEntry types.SegmentEntry) error {
	var entry segmentMapEntry
	var err error

	if entry.SegmentID, err = NewSegment(segmentEntry); err != nil {
		return err
	}
	entry.LastActionTime = tools.Timestamp()

	key := segmentKey{segmentEntry.UnifiedID, segmentEntry.ConjoinedPart,
		segmentEntry.SegmentNum}

	writer.SegmentMap[key] = entry

	return nil
}

func (writer *nimbusioWriter) StoreSequence(segmentEntry types.SegmentEntry,
	sequenceEntry types.SequenceEntry, data []byte) error {
	return fmt.Errorf("StoreSequence not implemented")
}
