package hosts

import (
	"database/sql"

	"webdirector/tools"
)

type hostsForCollectionImple struct {
	nodesStatement *sql.Stmt
}

// NewHostsForCollection returns an implementation of the
// HostsForCollection interface
func NewHostsForCollection() (HostsForCollection, error) {
	var err error
	var sqlDB *sql.DB

	if sqlDB, err = tools.OpenCentralDatabase(); err != nil {
		return nil, err
	}

	h := hostsForCollectionImple{}

	// TODO: the python version has a mirror database on memcache
	h.nodesStatement, err = sqlDB.Prepare(
		`select hostname from nimbusio_central.node where cluster_id = (
			select cluster_id from nimbusio_central.collection 
			where name = '?' and deletion_time is null)`)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h hostsForCollectionImple) GetHostNames(collectionName string) (
	[]string, error) {

	var hostNames []string
	var err error
	var rows *sql.Rows

	rows, err = h.nodesStatement.Query(collectionName)
	if err != nil {
		return hostNames, err
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
