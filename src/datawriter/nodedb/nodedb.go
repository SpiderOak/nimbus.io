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
	cancelSegment = `
       update nimbusio_node.segment
        set status = 'C'
        where unified_id = $1
        and conjoined_part = $2
        and segment_num = $3`
	cancelSegmentsFromNode = `
        update nimbusio_node.segment
        set status = 'C'
        where source_node_id = $1 
        and status = 'A' 
        and timestamp < $2`
	finishSegment = `
        update nimbusio_node.segment 
        set status = 'F',
            file_size = $1,
            file_adler32 = $2,
            file_hash = $3
        where id = $4`
	newTombstone = `
        insert into nimbusio_node.segment (
            collection_id,
            key,
            status,
            unified_id,
            timestamp,
            segment_num,
            source_node_id,
            handoff_node_id) 
        values ($1, $2, 'T', $3, $4, $5, $6, $7)`
	newTombstoneForUnifiedID = `
        insert into nimbusio_node.segment (
            collection_id,
            key,
            status,
            unified_id,
            timestamp,
            segment_num,
            file_tombstone_unified_id,
            source_node_id,
            handoff_node_id) 
        values ($1, $2, 'T', $3, $4, $5, $6, $7, $8)`
	newValueFile = `
        insert into nimbusio_node.value_file (space_id) values ($1) returning id`
	updateValueFile = `
	    update nimbusio_node.value_file set
            creation_time=$1,
            close_time=$2,
            size=$3,
            hash=$4,
            segment_sequence_count=$5,
            min_segment_id=$6,
            max_segment_id=$7,
            distinct_collection_count=$8,
            collection_ids=$9
        where id = $10`
	newSegmentSequence = `
        insert into nimbusio_node.segment_sequence (
            "collection_id",
            "segment_id",
            "zfec_padding_size",
            "value_file_id",
            "sequence_num",
            "value_file_offset",
            "size",
            "hash",
            "adler32"
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	newMetaData = `
        insert into nimbusio_node.meta (
            collection_id,
            segment_id,
            meta_key,
            meta_value,
            timestamp
        ) values ($1, $2, $3, $4, $5)`
	startConjoined = `
        insert into nimbusio_node.conjoined (
            collection_id, key, unified_id, create_timestamp, handoff_node_id
        ) values ($1, $2, $3, $4, $5)`
	abortConjoined = `
        update nimbusio_node.conjoined 
        set abort_timestamp = $1
        where collection_id = $2
        and key = $3
        and unified_id = $4
        and handoff_node_id is null`
	abortConjoinedForHandoffNodeID = `
        update nimbusio_node.conjoined 
        set abort_timestamp = $1
        where collection_id = $2
        and key = $3
        and unified_id = $4
        and handoff_node_id = $5`
	finishConjoined = `
        update nimbusio_node.conjoined 
        set complete_timestamp = $1
        where collection_id = $2
        and key = $3
        and unified_id = $4
        and handoff_node_id is null`
	finishConjoinedForHandoffNodeID = `
        update nimbusio_node.conjoined 
        set complete_timestamp = $1
        where collection_id = $2
        and key = $3
        and unified_id = $4
        and handoff_node_id = $5`
	deleteConjoined = `
        update nimbusio_node.conjoined 
        set delete_timestamp = $1
        where collection_id = $2
          and key = $3
          and unified_id < $4`
	deleteConjoinedForUnifiedID = `
        update nimbusio_node.conjoined 
        set delete_timestamp = $1
        where collection_id = $2
          and key = $3
          and unified_id = $4`
)

var (
	NodeDB *sql.DB
	Stmts  map[string]*sql.Stmt

	queryItems = []queryItem{
		queryItem{Name: "new-segment", Query: newSegment},
		queryItem{Name: "cancel-segment", Query: cancelSegment},
		queryItem{Name: "cancel-segments-from-node",
			Query: cancelSegmentsFromNode},
		queryItem{Name: "finish-segment", Query: finishSegment},
		queryItem{Name: "new-tombstone", Query: newTombstone},
		queryItem{Name: "new-tombstone-for-unified-id",
			Query: newTombstoneForUnifiedID},
		queryItem{Name: "new-value-file", Query: newValueFile},
		queryItem{Name: "update-value-file", Query: updateValueFile},
		queryItem{Name: "new-segment-sequence", Query: newSegmentSequence},
		queryItem{Name: "new-meta-data", Query: newMetaData},
		queryItem{Name: "start-conjoined", Query: startConjoined},
		queryItem{Name: "abort-conjoined", Query: abortConjoined},
		queryItem{Name: "abort-conjoined-for-handoff-node-id",
			Query: abortConjoinedForHandoffNodeID},
		queryItem{Name: "finish-conjoined", Query: finishConjoined},
		queryItem{Name: "finish-conjoined-for-handoff-node-id",
			Query: finishConjoinedForHandoffNodeID},
		queryItem{Name: "delete-conjoined", Query: deleteConjoined},
		queryItem{Name: "delete-conjoined-for-unified-id",
			Query: deleteConjoinedForUnifiedID}}
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
