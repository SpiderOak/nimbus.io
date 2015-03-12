package types

import (
	"time"
)

type CustomerKeyRow struct {
	ID           uint32    `json:"id"`
	CustomerID   uint32    `json:"customer_id"`
	Key          []byte    `json:"key"`
	Description  string    `json:"description"`
	CreationTime time.Time `json:"creation_time"`
	DeletionTime time.Time `json:"deletion_time"`
}
