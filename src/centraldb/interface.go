package centraldb

type CentralDB interface {

	// Close releases the resources held by the CentralDB
	Close()

	// GetHostsForCollection returns a slice of the host names that hold data
	// for the collection
	GetHostsForCollection(collectionName string) ([]string, error)
}
