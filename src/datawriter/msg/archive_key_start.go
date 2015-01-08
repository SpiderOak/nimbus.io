package msg

import (
	"encoding/json"
	"fmt"
)

type ArchiveKeyStart struct {
	UserRequestID string `json:"user-request-id"`
	ReturnAddress
	NodeNames
	Segment
	Sequence
}

func UnmarshalArchiveKeyStart(rawMessage string) (ArchiveKeyStart, error) {
	var result ArchiveKeyStart
	var err error

	err = json.Unmarshal([]byte(rawMessage), &result)
	if err != nil {
		return result, fmt.Errorf("json.Unmarshal %s", err)
	}

	return result, nil
}
