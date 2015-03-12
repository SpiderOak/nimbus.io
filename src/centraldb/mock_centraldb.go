package centraldb

import (
	"fmt"

	"types"
)

// MockCentralDB implements the CentralDB interface for testing
type MockCentralDB struct {
	CloseFunc                 func()
	GetHostsForCollectionFunc func(string) ([]string, error)
	GetNodeIDsForClusterFunc  func(string) (map[string]uint32, error)
	GetCollectionRowFunc      func(string) (types.CollectionRow, error)
	GetCustomerRowByNameFunc  func(string) (types.CustomerRow, error)
	GetCustomerRowByIDFunc    func(uint32) (types.CustomerRow, error)
	GetCustomerKeyRowFunc     func(uint32) (types.CustomerKeyRow, error)
}

// Close releases the resources held by the CentralDB
func (m MockCentralDB) Close() {
	if m.CloseFunc != nil {
		m.CloseFunc()
	}
}

// GetHostsForCollection returns a slice of the host names that hold data
// for the collection
func (m MockCentralDB) GetHostsForCollection(collectionName string) ([]string, error) {
	if m.GetHostsForCollectionFunc == nil {
		return nil, fmt.Errorf("GetHostsForCollection not implemented")
	}

	return m.GetHostsForCollectionFunc(collectionName)
}

// GetNodeIDsForCluster returns a map of node id keyed by node name,
// based on the cluster name
func (m MockCentralDB) GetNodeIDsForCluster(clusterName string) (map[string]uint32, error) {
	if m.GetNodeIDsForClusterFunc == nil {
		return nil, fmt.Errorf("GetNodeIDsForCluster not implemented")
	}

	return m.GetNodeIDsForClusterFunc(clusterName)
}

// GetCollectionRow returns the database row for the collection
func (m MockCentralDB) GetCollectionRow(collectionName string) (types.CollectionRow, error) {
	if m.GetCollectionRowFunc == nil {
		return types.CollectionRow{},
			fmt.Errorf("GetCollectionRow not implemented")
	}

	return m.GetCollectionRowFunc(collectionName)
}

// GetCustomerRowByName returns the customer row for a given name
func (m MockCentralDB) GetCustomerRowByName(customerName string) (types.CustomerRow, error) {
	if m.GetCustomerRowByNameFunc == nil {
		return types.CustomerRow{},
			fmt.Errorf("GetCustomerRowByName not implemented")
	}

	return m.GetCustomerRowByNameFunc(customerName)
}

// GetCustomerRowByID returns the customer row for a customer id
func (m MockCentralDB) GetCustomerRowByID(customerID uint32) (types.CustomerRow, error) {
	if m.GetCustomerRowByIDFunc == nil {
		return types.CustomerRow{},
			fmt.Errorf("GetCustomerRowByID not implemented")
	}

	return m.GetCustomerRowByIDFunc(customerID)
}

// GetCustomerKeyRow returns the customer row for a key id
func (m MockCentralDB) GetCustomerKeyRow(keyID uint32) (types.CustomerKeyRow, error) {
	if m.GetCustomerKeyRowFunc == nil {
		return types.CustomerKeyRow{},
			fmt.Errorf("GetCustomerKeyRow not implemented")
	}

	return m.GetCustomerKeyRowFunc(keyID)
}
