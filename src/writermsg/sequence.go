package writermsg

// Sequence contains data from messages on the sequence level within
// a segment
type Sequence struct {
	SequenceNum             uint32 `json:"sequence-num"`
	SegmentSize             uint64 `json:"segment-size"`
	ZfecPaddingSize         uint32 `json:"zfec-padding-size"`
	EncodedSegmentMD5Digest string `json:"segment-md5-digest"`
	SegmentAdler32          int32  `json:"segment-adler32"`
}
