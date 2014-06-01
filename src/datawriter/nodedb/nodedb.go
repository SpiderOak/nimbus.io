package nodedb

import (
	"database/sql"
	"fmt"

	"tools"
)

type queryItem struct {
	Name  string
	Query string
}

const (
	newSegment = `
        insert into nimbusio_node.segment (
            collection_id,
            key,
            status,
            unified_id,
            timestamp,
            segment_num,
            conjoined_part,
            source_node_id,
            handoff_node_id) 
        values ($1, $2, 'A', $3, $4, $5, $6, $7, $8) 
        returning id`
)

var (
	NodeDB *sql.DB
	Stmts  map[string]*sql.Stmt

	queryItems = []queryItem{
		queryItem{Name: "new-segment", Query: newSegment}}
)

// Initialize prepares the database for use
func Initialize() error {
	var err error

	Stmts = make(map[string]*sql.Stmt, len(queryItems))

	if NodeDB, err = tools.OpenLocalNodeDatabase(); err != nil {
		return err
	}

	for _, item := range queryItems {
		if Stmts[item.Name], err = NodeDB.Prepare(item.Query); err != nil {
			return fmt.Errorf("Prepare %s %s %s", item.Name, item.Query, err)
		}
	}

	return nil
}

func Close() {
	for key := range Stmts {
		stmt := Stmts[key]
		stmt.Close()
	}

	NodeDB.Close()
}
