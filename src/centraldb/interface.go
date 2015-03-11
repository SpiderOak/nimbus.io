package centraldb

import (
	"types"
)

type CentralDB interface {

	// Close releases the resources held by the CentralDB
	Close()

	// GetHostsForCollection returns a slice of the host names that hold data
	// for the collection
	GetHostsForCollection(collectionName string) ([]string, error)

	// GetNodeIDsForCluster returns a map of node id keyed by node name,
	// based on the cluster name
	GetNodeIDsForCluster(clusterName string) (map[string]uint32, error)

	// GetCollectionRow returns the database row for the collection
	GetCollectionRow(collectionName string) (types.CollectionRow, error)

	// GetCustomerRowByName returns the customer row for a given name
	GetCustomerRowByName(customerName string) (types.CustomerRow, error)

	// GetCustomerRowByID returns the customer row for a customer id
	GetCustomerRowByID(customerID uint32) (types.CustomerRow, error)

	// GetCustomerKeyRow returns the customer row for a key id
	GetCustomerKeyRow(keyID uint32) (types.CustomerKeyRow, error)
}
