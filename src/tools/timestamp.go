package tools

import (
	"time"
)

// Timestamp returns a standard (UTC) timestamp
func Timestamp() time.Time {
	return time.Time.UTC(time.Now())
}
