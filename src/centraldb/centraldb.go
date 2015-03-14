package centraldb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/bradfitz/gomemcache/memcache"

	"tools"
	"types"
)

const (
	requestChanCapacity         = 1024
	memcachedCentralKeyTemplate = "nimbusio_central_%s_by_%s_%s"
)

type centralDBImpl struct {
	requestChan chan<- interface{}
}

type getHostsForCollectionRequest struct {
	collectionName string
	resultChan     chan<- interface{}
}

type getNodeIDsForClusterRequest struct {
	clusterName string
	resultChan  chan<- interface{}
}

type getCollectionRowRequest struct {
	collectionName string
	resultChan     chan<- interface{}
}

type getCustomerRowByNameRequest struct {
	customerName string
	resultChan   chan<- interface{}
}

type getCustomerRowByIDRequest struct {
	customerID uint32
	resultChan chan<- interface{}
}

type getCustomerKeyRowRequest struct {
	keyID      uint32
	resultChan chan<- interface{}
}

var (
	DatabaseError  = errors.New("database error")
	sqlDB          *sql.DB
	memcacheClient *memcache.Client
	stmtMap        map[string]*sql.Stmt
	textMap        map[string]string
)

func NewCentralDB() CentralDB {
	var i centralDBImpl

	memcacheClient = tools.NewMemcacheClient()
	stmtMap = make(map[string]*sql.Stmt)
	textMap = map[string]string{
		"hosts-for-collection": `select hostname from nimbusio_central.node where cluster_id = (
				select cluster_id from nimbusio_central.collection 
				where name = $1 and deletion_time is null)
	          order by node_number_in_cluster`,
		"node-ids-for-cluster": `select id, name from nimbusio_central.node where cluster_id = (
			select id from nimbusio_central.cluster where name = $1`,
		"collection-row": `select id, name, customer_id, cluster_id, versioning, access_control, creation_time
			from nimbusio_central.collection where name = $1 
            and deletion_time is null`,
		"customer-row-by-name": `select id, username, creation_time
        	from nimbusio_central.customer 
            where username = $1 and deletion_time is null`,
		"customer-row-by-id": `select id, username, creation_time
        	from nimbusio_central.customer 
            where id = $1 and deletion_time is null`,
		"customer-key": `select id, customer_id, key, description, creation_time
        	from nimbusio_central.customer_key 
            where id = $1 and deletion_time is null`,
	}

	requestChan := make(chan interface{}, requestChanCapacity)
	i.requestChan = requestChan

	go func() {
		var err error

		for rawRequest := range requestChan {

			// if the database pointer is nil, attempt to open it
			// if we fail, we log an error and continue, leaving
			// the sqlDB pointer nil. Handler functions must
			// check for this
			if sqlDB == nil {
				if sqlDB, err = openCentralDatabase(); err != nil {
					log.Printf("error: central db: error in Open: %s", err)
				}
			}

			// if we think we have access to the database, send a ping
			// to verify
			if sqlDB != nil {
				if err = sqlDB.Ping(); err != nil {
					log.Printf("error: central db: error in Ping: %s", err)
					closeDB()
				}
			}

			switch request := rawRequest.(type) {
			case getHostsForCollectionRequest:
				handleGetHostsForCollection(request)
			case getNodeIDsForClusterRequest:
				handleGetNodeIDsForCluster(request)
			case getCollectionRowRequest:
				handleGetCollectionRow(request)
			case getCustomerRowByNameRequest:
				handleGetCustomerRowByName(request)
			case getCustomerRowByIDRequest:
				handleGetCustomerRowByID(request)
			case getCustomerKeyRowRequest:
				handleGetCustomerKeyRow(request)
			default:
				log.Printf("error: central db: unknown request type %T %q",
					request, request)
			}
		}
		log.Printf("debug: central db: end request loop")
		closeDB()
	}()

	return i
}

// GetNodeIDMap returns a map of node id keyed by node name, based on the
// NIMBUSIO_CLUSTER_NAME environment variable
func GetNodeIDMap() (map[string]uint32, error) {
	clusterName := os.Getenv("NIMBUSIO_CLUSTER_NAME")
	if clusterName == "" {
		return nil, fmt.Errorf("missing NIMBUSIO_CLUSTER_NAME")
	}

	centralDB := NewCentralDB()
	defer centralDB.Close()

	return centralDB.GetNodeIDsForCluster(clusterName)
}

func closeDB() {
	for key, value := range stmtMap {
		value.Close()
		delete(stmtMap, key)
	}
	sqlDB.Close()
	sqlDB = nil
}

func handleGetHostsForCollection(request getHostsForCollectionRequest) {
	log.Printf("debug: central db: handleGetHostsForCollection(%s)",
		request.collectionName)
	const stmtName = "hosts-for-collection"
	const memcacheKeyFormat = "nimbusio_central_hosts_for_%s"
	var hostNames []string
	var marshalledHostnames []byte
	var rows *sql.Rows
	var err error

	memcacheKey := fmt.Sprintf(memcacheKeyFormat, request.collectionName)

	item, err := memcacheClient.Get(memcacheKey)
	if err != nil {
		log.Printf("warning: unable to Get %s, %s", memcacheKey, err)
	} else {
		marshalledHostnames := item.Value
		err = json.Unmarshal(marshalledHostnames, &hostNames)
		if err != nil {
			log.Printf("warning: unable to unmarshal %s, %s", memcacheKey, err)
		} else {
			request.resultChan <- hostNames
			return
		}
	}

	if sqlDB == nil {
		request.resultChan <- DatabaseError
		return
	}

	stmt, err := getStmt(stmtName)
	if err != nil {
		log.Printf("error: preparing %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	rows, err = stmt.Query(request.collectionName)
	if err != nil {
		log.Printf("error: querying %s; %s", stmtName, err)
		removeStmt(stmtName, stmt)
		request.resultChan <- DatabaseError
		return
	}
	defer rows.Close()

	for rows.Next() {
		var hostName string
		if err = rows.Scan(&hostName); err != nil {
			log.Printf("error: scanning %s; %s", stmtName, err)
			request.resultChan <- DatabaseError
			return
		}
		hostNames = append(hostNames, hostName)
	}

	if err = rows.Err(); err != nil {
		log.Printf("error: rows.Err %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	marshalledHostnames, err = json.Marshal(hostNames)
	if err != nil {
		log.Printf("warning: unable to marshal %q: %s", hostNames, err)
	} else {
		err := memcacheClient.Set(&memcache.Item{Key: memcacheKey,
			Value: marshalledHostnames})
		if err != nil {
			log.Printf("warning: unable to Set %s, %s", memcacheKey, err)
		}
	}

	request.resultChan <- hostNames
}

func handleGetNodeIDsForCluster(request getNodeIDsForClusterRequest) {
	log.Printf("debug: central db: handleGetNodeIDsForCluster(%s)",
		request.clusterName)
	const stmtName = "node-ids-for-cluster"
	const memcacheKeyFormat = "nimbusio_node_ids_for_%s"
	nodeIDMap := make(map[string]uint32)
	var marshalledNodeIDMap []byte
	var rows *sql.Rows
	var err error

	memcacheKey := fmt.Sprintf(memcacheKeyFormat, request.clusterName)

	item, err := memcacheClient.Get(memcacheKey)
	if err != nil {
		log.Printf("warning: unable to Get %s, %s", memcacheKey, err)
	} else {
		marshalledNodeIDMap := item.Value
		err = json.Unmarshal(marshalledNodeIDMap, &nodeIDMap)
		if err != nil {
			log.Printf("warning: unable to unmarshal %s, %s", memcacheKey, err)
		} else {
			request.resultChan <- nodeIDMap
			return
		}
	}

	if sqlDB == nil {
		request.resultChan <- DatabaseError
		return
	}

	stmt, err := getStmt(stmtName)
	if err != nil {
		log.Printf("error: preparing %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	rows, err = stmt.Query(request.clusterName)
	if err != nil {
		log.Printf("error: querying %s; %s", stmtName, err)
		removeStmt(stmtName, stmt)
		request.resultChan <- DatabaseError
		return
	}
	defer rows.Close()

	for rows.Next() {
		var nodeID uint32
		var nodeName string
		if err := rows.Scan(&nodeID, &nodeName); err != nil {
			log.Printf("error: scanning %s; %s", stmtName, err)
			request.resultChan <- DatabaseError
			return
		}
		nodeIDMap[nodeName] = nodeID
	}

	if err = rows.Err(); err != nil {
		log.Printf("error: rows.Err %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	marshalledNodeIDMap, err = json.Marshal(nodeIDMap)
	if err != nil {
		log.Printf("warning: unable to marshal %q: %s", nodeIDMap, err)
	} else {
		err := memcacheClient.Set(&memcache.Item{Key: memcacheKey,
			Value: marshalledNodeIDMap})
		if err != nil {
			log.Printf("warning: unable to Set %s, %s", memcacheKey, err)
		}
	}

	request.resultChan <- nodeIDMap
}

func handleGetCollectionRow(request getCollectionRowRequest) {
	log.Printf("debug: central db: handleGetCollectionRow(%s)",
		request.collectionName)
	const stmtName = "collection-row"
	const memcacheKeyFormat = "nimbusio_central_collection_by_name_%s"
	var collectionRow types.CollectionRow
	var marshalledCollectionRow []byte
	var row *sql.Row
	var accessControl sql.NullString
	var err error

	memcacheKey := fmt.Sprintf(memcacheKeyFormat, request.collectionName)

	item, err := memcacheClient.Get(memcacheKey)
	if err != nil {
		log.Printf("warning: unable to Get %s, %s", memcacheKey, err)
	} else {
		marshalledCollectionRow := item.Value
		err = json.Unmarshal(marshalledCollectionRow, &collectionRow)
		if err != nil {
			log.Printf("warning: unable to unmarshal %s, %s", memcacheKey, err)
		} else {
			request.resultChan <- collectionRow
			return
		}
	}

	if sqlDB == nil {
		request.resultChan <- DatabaseError
		return
	}

	stmt, err := getStmt(stmtName)
	if err != nil {
		log.Printf("error: preparing %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	row = stmt.QueryRow(request.collectionName)
	err = row.Scan(&collectionRow.ID,
		&collectionRow.Name,
		&collectionRow.CustomerID,
		&collectionRow.ClusterID,
		&collectionRow.Versioning,
		&accessControl,
		&collectionRow.CreationTime)
	if err != nil {
		log.Printf("error: querying %s; %s", stmtName, err)
		removeStmt(stmtName, stmt)
		request.resultChan <- DatabaseError
		return
	}
	if accessControl.Valid {
		collectionRow.AccessControl = []byte(accessControl.String)
	}

	marshalledCollectionRow, err = json.Marshal(collectionRow)
	if err != nil {
		log.Printf("warning: unable to marshal %q: %s", collectionRow, err)
	} else {
		err := memcacheClient.Set(&memcache.Item{Key: memcacheKey,
			Value: marshalledCollectionRow})
		if err != nil {
			log.Printf("warning: unable to Set %s, %s", memcacheKey, err)
		}
	}

	request.resultChan <- collectionRow
}

func handleGetCustomerRowByName(request getCustomerRowByNameRequest) {
	log.Printf("debug: central db: handleGetCcustomerRowByName(%s)",
		request.customerName)
	const stmtName = "customer-row-by-name"
	const memcacheKeyFormat = "nimbusio_central_customer_by_name_%s"
	var customerRow types.CustomerRow
	var marshalledCustomerRow []byte
	var row *sql.Row
	var err error

	memcacheKey := fmt.Sprintf(memcacheKeyFormat, request.customerName)

	item, err := memcacheClient.Get(memcacheKey)
	if err != nil {
		log.Printf("warning: unable to Get %s, %s", memcacheKey, err)
	} else {
		marshalledCustomerRow := item.Value
		err = json.Unmarshal(marshalledCustomerRow, &customerRow)
		if err != nil {
			log.Printf("warning: unable to unmarshal %s, %s", memcacheKey, err)
		} else {
			request.resultChan <- customerRow
			return
		}
	}

	if sqlDB == nil {
		request.resultChan <- DatabaseError
		return
	}

	stmt, err := getStmt(stmtName)
	if err != nil {
		log.Printf("error: preparing %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	row = stmt.QueryRow(request.customerName)
	err = row.Scan(&customerRow.ID,
		&customerRow.UserName,
		&customerRow.CreationTime)
	if err != nil {
		log.Printf("error: querying %s; %s", stmtName, err)
		removeStmt(stmtName, stmt)
		request.resultChan <- DatabaseError
		return
	}

	marshalledCustomerRow, err = json.Marshal(customerRow)
	if err != nil {
		log.Printf("warning: unable to marshal %q: %s", customerRow, err)
	} else {
		err := memcacheClient.Set(&memcache.Item{Key: memcacheKey,
			Value: marshalledCustomerRow})
		if err != nil {
			log.Printf("warning: unable to Set %s, %s", memcacheKey, err)
		}
	}

	request.resultChan <- customerRow
}

func handleGetCustomerRowByID(request getCustomerRowByIDRequest) {
	log.Printf("debug: central db: handleGetCustomerRowByID(%d)",
		request.customerID)
	const stmtName = "customer-row-by-id"
	const memcacheKeyFormat = "nimbusio_central_customer_by_id_%d"
	var customerRow types.CustomerRow
	var marshalledCustomerRow []byte
	var row *sql.Row
	var err error

	memcacheKey := fmt.Sprintf(memcacheKeyFormat, request.customerID)

	item, err := memcacheClient.Get(memcacheKey)
	if err != nil {
		log.Printf("warning: unable to Get %s, %s", memcacheKey, err)
	} else {
		marshalledCustomerRow := item.Value
		err = json.Unmarshal(marshalledCustomerRow, &customerRow)
		if err != nil {
			log.Printf("warning: unable to unmarshal %s, %s", memcacheKey, err)
		} else {
			request.resultChan <- customerRow
			return
		}
	}

	if sqlDB == nil {
		request.resultChan <- DatabaseError
		return
	}

	stmt, err := getStmt(stmtName)
	if err != nil {
		log.Printf("error: preparing %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	row = stmt.QueryRow(request.customerID)
	err = row.Scan(&customerRow.ID,
		&customerRow.UserName,
		&customerRow.CreationTime)
	if err != nil {
		log.Printf("error: querying %s; %s", stmtName, err)
		removeStmt(stmtName, stmt)
		request.resultChan <- DatabaseError
		return
	}

	marshalledCustomerRow, err = json.Marshal(customerRow)
	if err != nil {
		log.Printf("warning: unable to marshal %q: %s", customerRow, err)
	} else {
		err := memcacheClient.Set(&memcache.Item{Key: memcacheKey,
			Value: marshalledCustomerRow})
		if err != nil {
			log.Printf("warning: unable to Set %s, %s", memcacheKey, err)
		}
	}

	request.resultChan <- customerRow
}

func handleGetCustomerKeyRow(request getCustomerKeyRowRequest) {
	log.Printf("debug: central db: handleGetCustomerKeyRow(%d)",
		request.keyID)
	const stmtName = "customer-key"
	const memcacheKeyFormat = "nimbusio_central_customer_key_by_id_%d"
	var customerKeyRow types.CustomerKeyRow
	var marshalledCustomerKeyRow []byte
	var row *sql.Row
	var description sql.NullString
	var err error

	memcacheKey := fmt.Sprintf(memcacheKeyFormat, request.keyID)

	item, err := memcacheClient.Get(memcacheKey)
	if err != nil {
		log.Printf("warning: unable to Get %s, %s", memcacheKey, err)
	} else {
		marshalledCustomerKeyRow := item.Value
		err = json.Unmarshal(marshalledCustomerKeyRow, &customerKeyRow)
		if err != nil {
			log.Printf("warning: unable to unmarshal %s, %s", memcacheKey, err)
		} else {
			request.resultChan <- customerKeyRow
			return
		}
	}

	if sqlDB == nil {
		request.resultChan <- DatabaseError
		return
	}

	stmt, err := getStmt(stmtName)
	if err != nil {
		log.Printf("error: preparing %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	row = stmt.QueryRow(request.keyID)
	err = row.Scan(&customerKeyRow.ID,
		&customerKeyRow.CustomerID,
		&customerKeyRow.Key,
		&description,
		&customerKeyRow.CreationTime)
	if err != nil {
		log.Printf("error: querying %s; %s", stmtName, err)
		removeStmt(stmtName, stmt)
		request.resultChan <- DatabaseError
		return
	}

	if description.Valid {
		customerKeyRow.Description = description.String
	}

	marshalledCustomerKeyRow, err = json.Marshal(customerKeyRow)
	if err != nil {
		log.Printf("warning: unable to marshal %q: %s", customerKeyRow, err)
	} else {
		err := memcacheClient.Set(&memcache.Item{Key: memcacheKey,
			Value: marshalledCustomerKeyRow})
		if err != nil {
			log.Printf("warning: unable to Set %s, %s", memcacheKey, err)
		}
	}

	request.resultChan <- customerKeyRow
}

func getStmt(name string) (*sql.Stmt, error) {
	var stmt *sql.Stmt
	var ok bool
	var err error

	if stmt, ok = stmtMap[name]; !ok {
		text, ok := textMap[name]
		if !ok {
			return nil, fmt.Errorf("unknown statement '%s'", name)
		}
		if stmt, err = sqlDB.Prepare(text); err != nil {
			return nil, err
		}
		stmtMap[name] = stmt
	}

	return stmt, nil
}

func removeStmt(name string, stmt *sql.Stmt) {
	if err := stmt.Close(); err != nil {
		log.Printf("warning: error closing statement %s; %s",
			name, err)
	}
	delete(stmtMap, name)
}

func (i centralDBImpl) Close() {
	log.Printf("debug: central db: Close")
	close(i.requestChan)
}

func (i centralDBImpl) GetHostsForCollection(collectionName string) (
	[]string, error) {
	log.Printf("debug: central db: GetHostsForCollection(%s)", collectionName)

	resultChan := make(chan interface{})
	request := getHostsForCollectionRequest{collectionName: collectionName,
		resultChan: resultChan}

	i.requestChan <- request
	rawResult := <-resultChan
	switch result := rawResult.(type) {
	case error:
		return nil, result
	case []string:
		return result, nil
	}

	log.Printf("error: GetHostsForCollection: unexpected result %T, %q",
		rawResult, rawResult)
	return nil, fmt.Errorf("Internal error in %s", "GetHostsForCollection")
}

func (i centralDBImpl) GetNodeIDsForCluster(clusterName string) (
	map[string]uint32, error) {
	log.Printf("debug: central db: GetNodeIDsForCluster(%s)", clusterName)

	resultChan := make(chan interface{})
	request := getNodeIDsForClusterRequest{clusterName: clusterName,
		resultChan: resultChan}

	i.requestChan <- request
	rawResult := <-resultChan
	switch result := rawResult.(type) {
	case error:
		return nil, result
	case map[string]uint32:
		return result, nil
	}

	log.Printf("error: GetNodeIDsForCluster: unexpected result %T, %q",
		rawResult, rawResult)
	return nil, fmt.Errorf("Internal error in %s", "GetNodeIDsForCluster")
}

// GetCollectionRow returns the database row for the collection
func (i centralDBImpl) GetCollectionRow(collectionName string) (
	types.CollectionRow, error) {
	log.Printf("debug: central db: GetCollectionRow(%s)", collectionName)

	resultChan := make(chan interface{})
	request := getCollectionRowRequest{collectionName: collectionName,
		resultChan: resultChan}

	i.requestChan <- request
	rawResult := <-resultChan
	switch result := rawResult.(type) {
	case error:
		return types.CollectionRow{}, result
	case types.CollectionRow:
		return result, nil
	}

	log.Printf("error: GetCollectionRow: unexpected result %T, %q",
		rawResult, rawResult)
	return types.CollectionRow{},
		fmt.Errorf("Internal error in %s", "GetCollectionRow")

}

// GetCustomerRowByName returns the database row for the customer
func (i centralDBImpl) GetCustomerRowByName(customerName string) (
	types.CustomerRow, error) {
	log.Printf("debug: central db: GetCustomerRowByName(%s)", customerName)

	resultChan := make(chan interface{})
	request := getCustomerRowByNameRequest{customerName: customerName,
		resultChan: resultChan}

	i.requestChan <- request
	rawResult := <-resultChan
	switch result := rawResult.(type) {
	case error:
		return types.CustomerRow{}, result
	case types.CustomerRow:
		return result, nil
	}

	log.Printf("error: GetCustomerRowByName: unexpected result %T, %q",
		rawResult, rawResult)
	return types.CustomerRow{},
		fmt.Errorf("Internal error in %s", "GetCustomerRowByName")

}

// GetCustomerRowByID returns the database row for the customer
func (i centralDBImpl) GetCustomerRowByID(customerID uint32) (
	types.CustomerRow, error) {
	log.Printf("debug: central db: GetCustomerRowByID(%d)", customerID)

	resultChan := make(chan interface{})
	request := getCustomerRowByIDRequest{customerID: customerID,
		resultChan: resultChan}

	i.requestChan <- request
	rawResult := <-resultChan
	switch result := rawResult.(type) {
	case error:
		return types.CustomerRow{}, result
	case types.CustomerRow:
		return result, nil
	}

	log.Printf("error: GetCustomerRowByID: unexpected result %T, %q",
		rawResult, rawResult)
	return types.CustomerRow{},
		fmt.Errorf("Internal error in %s", "GetCustomerRowByID")

}

// GetCustomerKeyRow returns the database row for the customer key
func (i centralDBImpl) GetCustomerKeyRow(keyID uint32) (
	types.CustomerKeyRow, error) {
	log.Printf("debug: central db: GetCustomerKeyRow(%d)", keyID)

	resultChan := make(chan interface{})
	request := getCustomerKeyRowRequest{keyID: keyID,
		resultChan: resultChan}

	i.requestChan <- request
	rawResult := <-resultChan
	switch result := rawResult.(type) {
	case error:
		return types.CustomerKeyRow{}, result
	case types.CustomerKeyRow:
		return result, nil
	}

	log.Printf("error: GetCustomerKeyRow: unexpected result %T, %q",
		rawResult, rawResult)
	return types.CustomerKeyRow{},
		fmt.Errorf("Internal error in %s", "GetCustomerKeyRow")

}
