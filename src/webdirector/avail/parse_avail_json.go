package avail

import (
	"encoding/json"

	"tools"
)

func parseAvailJSON(rawData []byte) (tools.HostAvail, error) {
	var hostAvail tools.HostAvail
	err := json.Unmarshal(rawData, &hostAvail)
	return hostAvail, err
}
