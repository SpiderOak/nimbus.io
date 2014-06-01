package writer

import (
	"fmt"

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

// map data contained in messages onto our internal segment id
type nimbusioWriter struct {
	segmentMap map[segmentKey]uint64
}

// NewNimbusioWriter returns an entity that implements the NimbusioWriter interface
func NewNimbusioWriter() NimbusioWriter {
	var writer nimbusioWriter

	writer.segmentMap = make(map[segmentKey]uint64)

	return &writer
}

func (writer *nimbusioWriter) StartSegment(segmentEntry types.SegmentEntry) error {
	return fmt.Errorf("StartSegment not implemented")
}

func (writer *nimbusioWriter) StoreSequence(segmentEntry types.SegmentEntry,
	sequenceEntry types.SequenceEntry, data []byte) error {
	return fmt.Errorf("StoreSequence not implemented")
}
