package types

import (
	"time"
)

type CustomerRow struct {
	ID           uint32    `json:"id"`
	UserName     string    `json:"user_name"`
	CreationTime time.Time `json:"creation_time"`
	DeletionTime time.Time `json:"deletion_time"`
}
