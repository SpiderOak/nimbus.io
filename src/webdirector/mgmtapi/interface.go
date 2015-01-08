package mgmtapi

type ManagementAPIDestinations interface {

	// Next returns the next destination to be used and advances the
	// round robin index
	Next() string
}
