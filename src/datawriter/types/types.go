package types

import (
	"fmt"
	"time"
)

// MessageMap is a raw message as it comes from JSON
type MessageMap map[string]interface{}

// Message is a partially parsed message, specifically with Type available
// for directing the message to a handler
type Message struct {
	Type          string
	ID            string
	ClientTag     string
	ClientAddress string
	UserRequestID string
	Map           MessageMap
	Data          []byte
}

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

// CancelEntry tells us to cancel a specific archive
type CancelEntry struct {
	UnifiedID     uint64
	ConjoinedPart uint32
	SegmentNum    uint8
}

func (entry CancelEntry) String() string {
	return fmt.Sprintf("%d %d %d",
		entry.UnifiedID,
		entry.ConjoinedPart,
		entry.SegmentNum)
}

// ConjoinedEntry identifies a conjoined upload
type ConjoinedEntry struct {
	CollectionID  uint32
	Key           string
	UnifiedID     uint64
	Timestamp     time.Time
	HandoffNodeID uint32
}

func (entry ConjoinedEntry) String() string {
	return fmt.Sprintf("%d %s %d %s %d",
		entry.CollectionID,
		entry.Key,
		entry.UnifiedID,
		entry.Timestamp,
		entry.HandoffNodeID)
}
