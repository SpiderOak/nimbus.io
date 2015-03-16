package writermsg

import (
	"encoding/json"
	"fmt"
)

type ReturnAddress struct {
	ClientTag     string `json:"client-tag"`
	ClientAddress string `json:"client-address"`
}

func UnmarshalReturnAddress(rawMessage string) (ReturnAddress, error) {
	var returnAddress ReturnAddress
	var err error

	err = json.Unmarshal([]byte(rawMessage), &returnAddress)
	if err != nil {
		return returnAddress, fmt.Errorf("json.Unmarshal %s", err)
	}

	return returnAddress, nil
}
