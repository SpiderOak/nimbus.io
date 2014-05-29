package tools

import (
	"testing"
)

func TestGetNodeIDMap(t *testing.T) {
	nodeIDMap, err := GetNodeIDMap()
	if err != nil {
		t.Fatalf("GetNodeIDMap() %s", err)
	}

	if len(nodeIDMap) != 10 {
		t.Fatalf("len(nodeIDMap) =  %d", len(nodeIDMap))
	}

	for key := range nodeIDMap {
		t.Logf("%s = %d", key, nodeIDMap[key])
	}
}
