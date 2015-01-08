package msg

import (
	"encoding/json"
	"fmt"
)

type ArchiveKeyFinal struct {
	UserRequestID string `json:"user-request-id"`
	ReturnAddress
	NodeNames
	Segment
	Sequence
	File
}

func UnmarshalArchiveKeyFinal(rawMessage string) (ArchiveKeyFinal, error) {
	var result ArchiveKeyFinal
	var err error

	err = json.Unmarshal([]byte(rawMessage), &result)
	if err != nil {
		return result, fmt.Errorf("json.Unmarshal %s", err)
	}

	return result, nil
}
