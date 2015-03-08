package types

import (
	"time"
)

type CollectionRow struct {
	ID            uint32    `json:"id"`
	Name          string    `json:"name"`
	CustomerID    uint32    `json:"customer_id"`
	ClusterID     uint32    `json:"cluster_id"`
	Versioning    bool      `json:"versioning"`
	AccessControl []byte    `json:"access_control"`
	CreationTime  time.Time `json:"creation_time"`
	DeletionTime  time.Time `json:"deletion_time"`
}
