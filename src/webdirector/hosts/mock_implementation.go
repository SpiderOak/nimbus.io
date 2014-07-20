package hosts

import (
	"fmt"
)

type mockHostForCollectionImple struct {
	collectionMap map[string][]string
}

// NewMockHostsForCollection returns a mock implementation of the
// HostsForCollection interface
func NewMockHostsForCollection(collectionMap map[string][]string) HostsForCollection {
	return mockHostForCollectionImple{collectionMap: collectionMap}
}

func (h mockHostForCollectionImple) GetHostNames(collectionName string) ([]string, error) {
	hostNames, ok := h.collectionMap[collectionName]
	if !ok {
		return nil, fmt.Errorf("no hosts for %s", collectionName)
	}

	return hostNames, nil
}
