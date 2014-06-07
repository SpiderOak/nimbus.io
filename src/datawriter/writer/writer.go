package writer

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"tools"

	"datawriter/logger"
	"datawriter/nodedb"
	"datawriter/types"
)

type NimbusioWriter interface {

	// StartSegment initializes a new segment and prepares to receive data
	// for it
	StartSegment(lgr logger.Logger, segmentEntry types.SegmentEntry) error

	// StoreSequence stores data for  an initialized segment
	StoreSequence(lgr logger.Logger, segmentEntry types.SegmentEntry,
		sequenceEntry types.SequenceEntry, data []byte) error

	// CancelSegment stops processing the segment
	CancelSegment(lgr logger.Logger, cancelEntry types.CancelEntry) error

	// FinishSegment finishes storing the segment
	FinishSegment(lgr logger.Logger, segmentEntry types.SegmentEntry,
		fileEntry types.FileEntry) error

	// DestroyKey makes a key inaccessible
	DestroyKey(lgr logger.Logger, segmentEntry types.SegmentEntry,
		unifiedIDToDestroy uint64) error
}

type segmentKey struct {
	UnifiedID     uint64
	ConjoinedPart uint32
	SegmentNum    uint8
}

func (key segmentKey) String() string {
	return fmt.Sprintf("(%d, %d, %d)", key.UnifiedID, key.ConjoinedPart,
		key.SegmentNum)
}

type segmentMapEntry struct {
	SegmentID      uint64
	LastActionTime time.Time
}

// map data contained in messages onto our internal segment id
type nimbusioWriter struct {
	SegmentMap       map[segmentKey]segmentMapEntry
	FileSpaceInfo    tools.FileSpaceInfo
	ValueFile        OutputValueFile
	MaxValueFileSize uint64
}

// NewNimbusioWriter returns an entity that implements the NimbusioWriter interface
func NewNimbusioWriter() (NimbusioWriter, error) {
	var err error
	var writer nimbusioWriter
	writer.SegmentMap = make(map[segmentKey]segmentMapEntry)

	maxValueFileSizeStr := os.Getenv("NIMBUS_IO_MAX_VALUE_FILE_SIZE")
	if maxValueFileSizeStr == "" {
		writer.MaxValueFileSize = uint64(1024 * 1024 * 1024)
	} else {
		var intSize int
		intSize, err = strconv.Atoi(maxValueFileSizeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid NIMBUS_IO_MAX_VALUE_FILE_SIZE '%s'",
				maxValueFileSizeStr)
		}
		writer.MaxValueFileSize = uint64(intSize)
	}

	if writer.FileSpaceInfo, err = tools.NewFileSpaceInfo(nodedb.NodeDB); err != nil {
		return nil, err
	}

	if writer.ValueFile, err = NewOutputValueFile(writer.FileSpaceInfo); err != nil {
		return nil, err
	}

	return &writer, nil
}

func (writer *nimbusioWriter) StartSegment(lgr logger.Logger,
	segmentEntry types.SegmentEntry) error {
	var entry segmentMapEntry
	var err error

	lgr.Debug("StartSegment")

	if entry.SegmentID, err = NewSegment(segmentEntry); err != nil {
		return err
	}
	entry.LastActionTime = tools.Timestamp()

	key := segmentKey{segmentEntry.UnifiedID, segmentEntry.ConjoinedPart,
		segmentEntry.SegmentNum}

	writer.SegmentMap[key] = entry

	return nil
}

func (writer *nimbusioWriter) StoreSequence(lgr logger.Logger,
	segmentEntry types.SegmentEntry,
	sequenceEntry types.SequenceEntry, data []byte) error {
	var err error

	lgr.Debug("StoreSequence #%d", sequenceEntry.SequenceNum)

	if writer.ValueFile.Size()+sequenceEntry.SegmentSize >= writer.MaxValueFileSize {
		lgr.Info("value file full")
		if err = writer.ValueFile.Close(); err != nil {
			return fmt.Errorf("error closing value file %s", err)
		}
		if writer.ValueFile, err = NewOutputValueFile(writer.FileSpaceInfo); err != nil {
			return fmt.Errorf("error opening value file %s", err)
		}
	}

	key := segmentKey{segmentEntry.UnifiedID, segmentEntry.ConjoinedPart,
		segmentEntry.SegmentNum}
	entry, ok := writer.SegmentMap[key]
	if !ok {
		return fmt.Errorf("StoreSequence unknown segment %s", key)
	}

	// we need to store new-segment-sequence in the database before
	// ValueFile.Store, because we are using  writer.ValueFile.Size()
	// as the offset

	stmt := nodedb.Stmts["new-segment-sequence"]
	_, err = stmt.Exec(
		segmentEntry.CollectionID,
		entry.SegmentID,
		sequenceEntry.ZfecPaddingSize,
		writer.ValueFile.ID(),
		sequenceEntry.SequenceNum,
		writer.ValueFile.Size(),
		sequenceEntry.SegmentSize,
		sequenceEntry.MD5Digest,
		sequenceEntry.Adler32)
	if err != nil {
		return fmt.Errorf("new-segment-sequence %s", err)
	}

	err = writer.ValueFile.Store(segmentEntry.CollectionID, entry.SegmentID,
		data)
	if err != nil {
		return fmt.Errorf("ValueFile.Store %s", err)
	}

	entry.LastActionTime = tools.Timestamp()
	writer.SegmentMap[key] = entry

	return nil
}

// CancelSegment stops storing the segment
func (writer *nimbusioWriter) CancelSegment(lgr logger.Logger,
	cancelEntry types.CancelEntry) error {
	var err error

	lgr.Debug("CancelSegment")

	key := segmentKey{cancelEntry.UnifiedID, cancelEntry.ConjoinedPart,
		cancelEntry.SegmentNum}
	delete(writer.SegmentMap, key)

	stmt := nodedb.Stmts["cancel-segment"]
	_, err = stmt.Exec(
		cancelEntry.UnifiedID,
		cancelEntry.ConjoinedPart,
		cancelEntry.SegmentNum)

	if err != nil {
		return fmt.Errorf("cancel-segment %s", err)
	}

	return nil
}

// FinishSegment finishes storing the segment
func (writer *nimbusioWriter) FinishSegment(lgr logger.Logger,
	segmentEntry types.SegmentEntry, fileEntry types.FileEntry) error {
	var err error

	lgr.Debug("FinishSegment")

	key := segmentKey{segmentEntry.UnifiedID, segmentEntry.ConjoinedPart,
		segmentEntry.SegmentNum}
	entry, ok := writer.SegmentMap[key]
	if !ok {
		return fmt.Errorf("FinishSegment unknown segment %s", key)
	}

	delete(writer.SegmentMap, key)

	stmt := nodedb.Stmts["finish-segment"]
	_, err = stmt.Exec(
		fileEntry.FileSize,
		fileEntry.Adler32,
		fileEntry.MD5Digest,
		entry.SegmentID)

	if err != nil {
		return fmt.Errorf("finish-segment %s", err)
	}

	for _, metaEntry := range fileEntry.MetaData {
		stmt := nodedb.Stmts["new-meta-data"]
		_, err = stmt.Exec(
			segmentEntry.CollectionID,
			entry.SegmentID,
			metaEntry.Key,
			metaEntry.Value,
			segmentEntry.Timestamp)

		if err != nil {
			return fmt.Errorf("new-meta-data %s", err)
		}
	}

	return nil
}

// DestroyKey makes a key inaccessible
func (writer *nimbusioWriter) DestroyKey(lgr logger.Logger,
	segmentEntry types.SegmentEntry,
	unifiedIDToDestroy uint64) error {

	var err error

	lgr.Debug("DestroyKey (%d)", unifiedIDToDestroy)

	if unifiedIDToDestroy > 0 {
		stmt := nodedb.Stmts["new-tombstone-for-unified-id"]
		_, err = stmt.Exec(
			segmentEntry.CollectionID,
			segmentEntry.Key,
			segmentEntry.UnifiedID,
			segmentEntry.Timestamp,
			segmentEntry.SegmentNum,
			unifiedIDToDestroy,
			segmentEntry.SourceNodeID,
			segmentEntry.HandoffNodeID)

		if err != nil {
			return fmt.Errorf("new-tombstone-for-unified-id %d %s",
				unifiedIDToDestroy, err)
		}

		stmt = nodedb.Stmts["delete-conjoined-for-unified-id"]
		_, err = stmt.Exec(
			segmentEntry.Timestamp,
			segmentEntry.CollectionID,
			segmentEntry.Key,
			unifiedIDToDestroy)

		if err != nil {
			return fmt.Errorf("delete-conjoined-for-unified-id %d %s",
				unifiedIDToDestroy, err)
		}
	} else {
		stmt := nodedb.Stmts["new-tombstone"]
		_, err = stmt.Exec(
			segmentEntry.CollectionID,
			segmentEntry.Key,
			segmentEntry.UnifiedID,
			segmentEntry.Timestamp,
			segmentEntry.SegmentNum,
			segmentEntry.SourceNodeID,
			segmentEntry.HandoffNodeID)

		if err != nil {
			return fmt.Errorf("new-tombstone %s", err)
		}

		stmt = nodedb.Stmts["delete-conjoined"]
		_, err = stmt.Exec(
			segmentEntry.Timestamp,
			segmentEntry.CollectionID,
			segmentEntry.Key,
			segmentEntry.UnifiedID)

		if err != nil {
			return fmt.Errorf("delete-conjoined %s", err)
		}
	}
	// Set delete_timestamp on all conjoined rows for this key
	// that are older than this tombstone

	return nil
}
