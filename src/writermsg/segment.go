package writermsg

// Segment contains message data referring to a specific segment
type Segment struct {
	CollectionID  uint32 `json:"collection-id"`
	Key           string `json:"key"`
	UnifiedID     uint64 `json:"unified-id"`
	TimestampRepr string `json:"timestamp-repr"`
	ConjoinedPart uint32 `json:"conjoined-part"`
	SegmentNum    uint8  `json:"segment-num"`
}
