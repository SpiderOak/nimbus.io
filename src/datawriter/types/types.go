package types

import (
	"fmt"
	"time"
)

// Message is an incoming message
type Message struct {
	Type       string
	ID         string
	Marshalled string
	Data       []byte
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
