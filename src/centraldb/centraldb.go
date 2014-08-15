package centraldb

import (
	"database/sql"
	"errors"
	"fmt"

	"fog"
)

const (
	requestChanCapacity = 1024
)

type centralDBImpl struct {
	requestChan chan<- interface{}
}

type getHostsForCollectionRequest struct {
	collectionName string
	resultChan     chan<- interface{}
}

var (
	DatabaseError = errors.New("database error")
	sqlDB         *sql.DB
	stmtMap       map[string]*sql.Stmt
	textMap       map[string]string
)

func NewCentralDB() CentralDB {
	var i centralDBImpl

	stmtMap = make(map[string]*sql.Stmt)
	textMap = map[string]string{
		"hosts-for-collection": `select hostname from nimbusio_central.node where cluster_id = (
				select cluster_id from nimbusio_central.collection 
				where name = $1 and deletion_time is null)
	          order by node_number_in_cluster`}

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
					fog.Error("central db: error in Open: %s", err)
				}
			}

			// if we think we have access to the database, send a ping
			// to verify
			if sqlDB != nil {
				if err = sqlDB.Ping(); err != nil {
					fog.Error("central db: error in Ping: %s", err)
					closeDB()
				}
			}

			switch request := rawRequest.(type) {
			case getHostsForCollectionRequest:
				handleGetHostsForCollection(request)
			default:
				fog.Error("central db: unknown request type %T %q",
					request, request)
			}
		}
		fog.Debug("central db: end request loop")
		closeDB()
	}()

	return i
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
	fog.Debug("central db: handleGetHostsForCollection(%s)",
		request.collectionName)
	const stmtName = "hosts-for-collection"

	if sqlDB == nil {
		request.resultChan <- DatabaseError
		return
	}

	stmt, err := getStmt(stmtName)
	if err != nil {
		fog.Error("Error preparing %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	var hostNames []string
	var rows *sql.Rows

	rows, err = stmt.Query(request.collectionName)
	if err != nil {
		fog.Error("Error querying %s; %s", stmtName, err)
		removeStmt(stmtName, stmt)
		request.resultChan <- DatabaseError
		return
	}
	defer rows.Close()

	for rows.Next() {
		var hostName string
		if err = rows.Scan(&hostName); err != nil {
			fog.Error("Error scanning %s; %s", stmtName, err)
			request.resultChan <- DatabaseError
			return
		}
		hostNames = append(hostNames, hostName)
	}

	if err = rows.Err(); err != nil {
		fog.Error("rows.Err %s; %s", stmtName, err)
		request.resultChan <- DatabaseError
		return
	}

	request.resultChan <- hostNames
}

func getStmt(name string) (*sql.Stmt, error) {
	var stmt *sql.Stmt
	var err error

	if stmt, ok := stmtMap[name]; !ok {
		text := textMap[name]
		if stmt, err = sqlDB.Prepare(text); err != nil {
			return nil, err
		}
		stmtMap[name] = stmt
	}

	return stmt, nil
}

func removeStmt(name string, stmt *sql.Stmt) {
	if err := stmt.Close(); err != nil {
		fog.Warn("error closing statement %s; %s",
			name, err)
	}
	delete(stmtMap, name)
}

func (i centralDBImpl) Close() {
	fog.Debug("central db: Close")
	close(i.requestChan)
}

func (i centralDBImpl) GetHostsForCollection(collectionName string) (
	[]string, error) {
	fog.Debug("central db: GetHostsForCollection(%s)", collectionName)

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

	fog.Error("GetHostsForCollection: unexpected result %T, %q", rawResult, rawResult)
	return nil, fmt.Errorf("Internal error in %s", "GetHostsForCollection")
}
