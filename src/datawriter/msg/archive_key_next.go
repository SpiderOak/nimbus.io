package msg

import (
	"encoding/json"
	"fmt"
)

type ArchiveKeyNext struct {
	UserRequestID string `json:"user-request-id"`
	ReturnAddress
	NodeNames
	Segment
	Sequence
}

func UnmarshalArchiveKeyNext(rawMessage string) (ArchiveKeyNext, error) {
	var result ArchiveKeyNext
	var err error

	err = json.Unmarshal([]byte(rawMessage), &result)
	if err != nil {
		return result, fmt.Errorf("json.Unmarshal %s", err)
	}

	return result, nil
}
