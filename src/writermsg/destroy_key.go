package writermsg

import (
	"encoding/json"
	"fmt"
)

type DestroyKey struct {
	UserRequestID      string `json:"user-request-id"`
	UnifiedIDToDestroy uint64 `json:"unified-id-to-delete"`
	ReturnAddress
	NodeNames
	Segment
}

func UnmarshalDestroyKey(rawMessage string) (DestroyKey, error) {
	var result DestroyKey
	var err error

	err = json.Unmarshal([]byte(rawMessage), &result)
	if err != nil {
		return result, fmt.Errorf("json.Unmarshal %s", err)
	}

	return result, nil
}
