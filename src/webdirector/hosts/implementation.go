package hosts

import (
	"database/sql"
	"fmt"

	"fog"
	"tools"
)

// HostsDatabaseError implements the error interface. The caller can
// use it to differentiate database errors from collection errors
// see https://treehouse.spideroak.com/pandora/ticket/5389#comment:3
type HostsDatabaseError struct {
	err error
}

type hostsForCollectionImple struct {
	nodesStatement *sql.Stmt
}

// NewHostsForCollection returns an implementation of the
// HostsForCollection interface
func NewHostsForCollection() HostsForCollection {
	// we do not fail here.
	// see https://treehouse.spideroak.com/pandora/ticket/5389#comment:3
	return &hostsForCollectionImple{}
}

func (h *hostsForCollectionImple) GetHostNames(collectionName string) (
	[]string, error) {
	var err error

	if h.nodesStatement == nil {
		var sqlDB *sql.DB

		if sqlDB, err = tools.OpenCentralDatabase(); err != nil {
			return nil, HostsDatabaseError{err: err}
		}

		// TODO: the python version has a mirror database on memcache
		h.nodesStatement, err = sqlDB.Prepare(
			`select hostname from nimbusio_central.node where cluster_id = (
				select cluster_id from nimbusio_central.collection 
				where name = $1 and deletion_time is null)
	          order by node_number_in_cluster`)
		if err != nil {
			return nil, HostsDatabaseError{err: err}
		}
	}

	var hostNames []string
	var rows *sql.Rows

	rows, err = h.nodesStatement.Query(collectionName)
	if err != nil {
		if closeErr := h.nodesStatement.Close(); closeErr != nil {
			fog.Warn("GetHostNames: error closing statement %s after %s",
				closeErr, err)
		}
		h.nodesStatement = nil
		return hostNames, HostsDatabaseError{err: err}
	}
	defer rows.Close()

	for rows.Next() {
		var hostName string
		if err = rows.Scan(&hostName); err != nil {
			return hostNames, err
		}
		hostNames = append(hostNames, hostName)
	}

	if err = rows.Err(); err != nil {
		return hostNames, err
	}

	return hostNames, nil
}

func (h HostsDatabaseError) Error() string {
	return fmt.Sprintf("database error in GetHostNames %s", h.err)
}
