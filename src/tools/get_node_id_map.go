package tools

import (
	"fmt"
	"os"
)

// GetNodeIDMap returns a map of node id keyed by node name, based on the
// NIMBUSIO_CLUSTER_NAME environment variable
func GetNodeIDMap() (map[string]uint32, error) {
	nodeIDMap := make(map[string]uint32)

	clusterName := os.Getenv("NIMBUSIO_CLUSTER_NAME")
	if clusterName == "" {
		return nodeIDMap, fmt.Errorf("missing NIMBUSIO_CLUSTER_NAME")
	}

	sqlDB, err := OpenCentralDatabase()
	if err != nil {
		return nodeIDMap, err
	}
	defer sqlDB.Close()

	rows, err := sqlDB.Query(
		`select id, name from nimbusio_central.node where cluster_id = (
			select id from nimbusio_central.cluster where name = $1)`,
		clusterName)
	if err != nil {
		return nodeIDMap, err
	}

	for rows.Next() {
		var nodeID uint32
		var nodeName string
		if err := rows.Scan(&nodeID, &nodeName); err != nil {
			return nodeIDMap, err
		}
		nodeIDMap[nodeName] = nodeID
	}

	if err := rows.Err(); err != nil {
		return nodeIDMap, err
	}

	return nodeIDMap, nil
}
