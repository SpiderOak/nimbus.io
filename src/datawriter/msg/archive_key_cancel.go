package msg

import (
	"encoding/json"
	"fmt"
)

// ArchiveKeyCancel contains a chancellation request for a segment
type ArchiveKeyCancel struct {
	UserRequestID string `json:"user-request-id"`
	UnifiedID     uint64 `json:"unified-id"`
	ConjoinedPart uint32 `json:"conjoined-part"`
	SegmentNum    uint8  `json:"segment-num"`
	ReturnAddress
}

func UnmarshalArchiveKeyCancel(rawMessage string) (ArchiveKeyCancel, error) {
	var result ArchiveKeyCancel
	var err error

	err = json.Unmarshal([]byte(rawMessage), &result)
	if err != nil {
		return result, fmt.Errorf("json.Unmarshal %s", err)
	}

	return result, nil
}
