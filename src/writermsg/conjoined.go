package writermsg

import (
	"encoding/json"
	"fmt"
)

type Conjoined struct {
	UserRequestID   string `json:"user-request-id"`
	CollectionID    uint32 `json:"collection-id"`
	Key             string `json:"key"`
	UnifiedID       uint64 `json:"unified-id"`
	TimestampRepr   string `json:"timestamp-repr"`
	HandoffNodeName string `json:"handoff-node-name"`
	ReturnAddress
}

func UnmarshalConjoined(rawMessage string) (Conjoined, error) {
	var result Conjoined
	var err error

	err = json.Unmarshal([]byte(rawMessage), &result)
	if err != nil {
		return result, fmt.Errorf("json.Unmarshal %s", err)
	}

	return result, nil
}
