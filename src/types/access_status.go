package access

import (
	"fmt"
)

type AccessStatus uint8

const (
	_               = iota
	None AccessType = iota
	Allowed
	RequiresPasswordAuthentication
	Forbidden
)

func (r AccessStatus) String() string {
	switch r {
	case Allowed:
		return "Allowed"
	case RequiresPasswordAuthentication:
		return "RequiresPasswordAuthentication"
	case Forbidden:
		return "Forbidden"
	default:
		return fmt.Sprintf("unknown status (%d)", int64(r))
	}
}
