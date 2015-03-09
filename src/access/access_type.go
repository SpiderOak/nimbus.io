package access

import (
	"fmt"
)

type AccessType uint8

const (
	NoAccess AccessType = iota
	Read
	Write
	List
	Delete
)

func (r AccessType) String() string {
	switch r {
	case Read:
		return "Read"
	case Write:
		return "Write"
	case List:
		return "List"
	case Delete:
		return "Delete"
	default:
		return fmt.Sprintf("unknown access (%d)", int64(r))
	}
}
