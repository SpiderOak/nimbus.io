package avail

import (
	"encoding/json"
)

type HostAvail struct {
	Reachable      bool    `json:"reachable"`
	TimestampFloat float64 `json:"timestamp"`
}

func parseAvailJSON(rawData []byte) (HostAvail, error) {
	var hostAvail HostAvail
	err := json.Unmarshal(rawData, &hostAvail)
	return hostAvail, err
}
