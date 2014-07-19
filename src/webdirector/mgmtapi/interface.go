package mgmtapi

type ManagementAPIDestinations interface {

	// Next returns the next destination to be used and advances
	Next() string

	// Peek returns the next destination that will be used
	// This is intended for convenience in testing
	Peek() string
}
