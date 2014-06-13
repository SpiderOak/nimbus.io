package msg

import (
	"encoding/json"
	"fmt"
)

type ArchiveKeyEntire struct {
	UserRequestID string `json:"user_request_id"`
	ReturnAddress
	NodeNames
	Segment
	Sequence
	File
}

func UnmarshalArchiveKeyEntire(rawMessage string) (ArchiveKeyEntire, error) {
	var result ArchiveKeyEntire
	var err error

	err = json.Unmarshal([]byte(rawMessage), &result)
	if err != nil {
		return result, fmt.Errorf("json.Unmarshal %s", err)
	}

	return result, nil
}
