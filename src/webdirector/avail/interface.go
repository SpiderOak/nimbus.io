package avail

// Availability reports the availability of hosts
type Availability interface {

	// AvailableHosts returns a slice of the incoming hostnames
	// showing which ones are available
	AvailableHosts(hostNames []string, destPort string) ([]string, error)
}
