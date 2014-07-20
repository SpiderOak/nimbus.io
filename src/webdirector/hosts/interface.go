package hosts

type HostsForCollection interface {
	GetHostNames(collectionName string) ([]string, error)
}
