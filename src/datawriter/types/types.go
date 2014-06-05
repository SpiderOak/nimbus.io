package types

import (
	"fmt"
	"time"
)

// SegmentEntry contains the data from messages identifying segment level
// information
type SegmentEntry struct {
	CollectionID  uint32
	Key           string
	UnifiedID     uint64
	Timestamp     time.Time
	ConjoinedPart uint32
	SegmentNum    uint8
	SourceNodeID  uint32
	HandoffNodeID uint32
}

func (entry SegmentEntry) String() string {
	return fmt.Sprintf("(%d) %s %d %s %d %d",
		entry.CollectionID,
		entry.Key,
		entry.UnifiedID,
		entry.Timestamp,
		entry.ConjoinedPart,
		entry.SegmentNum)
}

// Sequence entry contains data from messages on the sequence level within
// a segment
type SequenceEntry struct {
	SequenceNum     uint32
	SegmentSize     uint64
	ZfecPaddingSize uint32
	MD5Digest       []byte
	Adler32         int32
}

func (entry SequenceEntry) String() string {
	return fmt.Sprintf("%d %d %d %x %d",
		entry.SequenceNum,
		entry.SegmentSize,
		entry.ZfecPaddingSize,
		entry.MD5Digest,
		entry.Adler32)
}

// MetaEntry meta data for a segment
type MetaEntry struct {
	Key   string
	Value string
}

// FileEntry for a segment
type FileEntry struct {
	FileSize  uint64
	MD5Digest []byte
	Adler32   int32
	MetaData  []MetaEntry
}

func (entry FileEntry) String() string {
	return fmt.Sprintf("%d %x %d [%s]",
		entry.FileSize,
		entry.MD5Digest,
		entry.Adler32,
		entry.MetaData)
}
