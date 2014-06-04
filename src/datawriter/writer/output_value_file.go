package writer

import (
	"crypto/md5"
	"fmt"
	"hash"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"tools"

	"fog"

	"datawriter/nodedb"
)

type OutputValueFile interface {
	// Size returns the amount of data written so far
	Size() uint64

	// Store the data for one sequence
	Store(collectionID uint32, segmentID uint64, data []byte) error

	// Close the underling file and update the database row
	Close() error
}

type outputValueFile struct {
	creationTime     time.Time
	spaceID          uint32
	valueFileID      uint32
	bytesWritten     uint64
	sequencesWritten int
	md5Sum           hash.Hash
	minSegmentID     uint64
	maxSegmentID     uint64
	collectionIDSet  map[uint32]struct{}
	filePath         string
	fileHandle       *os.File
}

// NewOutputValueFile creates an entity implmenting the OutputValueFile interface
func NewOutputValueFile(fileSpaceInfo tools.FileSpaceInfo) (OutputValueFile, error) {
	var valueFile outputValueFile
	var err error

	valueFile.creationTime = tools.Timestamp()
	valueFile.md5Sum = md5.New()
	valueFile.collectionIDSet = make(map[uint32]struct{})
	repositoryPath := os.Getenv("NIMBUSIO_REPOSITORY_PATH")

	if valueFile.spaceID, err = fileSpaceInfo.FindMaxAvailSpaceID(tools.FileSpaceJournal); err != nil {
		return nil, err
	}

	if err = valueFile.insertValueFileRow(); err != nil {
		return nil, err
	}

	valueFile.filePath = tools.ComputeValueFilePath(repositoryPath, valueFile.spaceID,
		valueFile.valueFileID)

	fog.Debug("NewOutputValueFile %s", valueFile.filePath)

	dirPath := path.Dir(valueFile.filePath)
	if err = os.MkdirAll(dirPath, os.ModeDir|0755); err != nil {
		return nil, fmt.Errorf("os.MkdirAll(%s...", err)
	}

	valueFile.fileHandle, err = os.Create(valueFile.filePath)
	if err != nil {
		return nil, fmt.Errorf("os.Create(%s) %s", valueFile.filePath, err)
	}

	return &valueFile, nil
}

// Size returns the amount of data written so far
func (valueFile *outputValueFile) Size() uint64 {
	return valueFile.bytesWritten
}

// Store the data for one sequence
func (valueFile *outputValueFile) Store(collectionID uint32, segmentID uint64,
	data []byte) error {
	var err error

	if _, err = valueFile.fileHandle.Write(data); err != nil {
		return err
	}
	valueFile.md5Sum.Write(data)

	valueFile.bytesWritten += uint64(len(data))
	valueFile.sequencesWritten += 1

	if valueFile.maxSegmentID == 0 {
		valueFile.minSegmentID = segmentID
		valueFile.maxSegmentID = segmentID
	} else {
		valueFile.minSegmentID = min(valueFile.minSegmentID, segmentID)
		valueFile.maxSegmentID = max(valueFile.maxSegmentID, segmentID)
	}

	valueFile.collectionIDSet[collectionID] = struct{}{}

	return nil
}

// Close the underling file and update the database row
func (valueFile *outputValueFile) Close() error {
	if err := valueFile.fileHandle.Close(); err != nil {
		return fmt.Errorf("Close() %s %s", valueFile.filePath, err)
	}

	if valueFile.bytesWritten == 0 {
		fog.Debug("OutputValueFile removing empty file %s", valueFile.filePath)
		return os.Remove(valueFile.filePath)
	}

	if err := valueFile.updateValueFileRow(); err != nil {
		return err
	}

	return nil
}

func (valueFile *outputValueFile) insertValueFileRow() error {
	stmt := nodedb.Stmts["new-value-file"]
	row := stmt.QueryRow(valueFile.spaceID)
	err := row.Scan(&valueFile.valueFileID)
	return err
}

func (valueFile *outputValueFile) updateValueFileRow() error {
	closeTime := tools.Timestamp()
	md5Digest := valueFile.md5Sum.Sum(nil)

	// convert the set of collection ids to a form postgres will take
	// {n1, n1, ...}
	collectionIDs := make([]int, len(valueFile.collectionIDSet))
	var n int
	for collectionID := range valueFile.collectionIDSet {
		collectionIDs[n] = int(collectionID)
		n += 1
	}
	sort.Ints(collectionIDs)
	collectionIDStrings := make([]string, len(collectionIDs))
	for n, collectionID := range collectionIDs {
		collectionIDStrings[n] = fmt.Sprintf("%d", collectionID)
	}
	collectionIDLiteral := fmt.Sprintf("{%s}",
		strings.Join(collectionIDStrings, ","))

	stmt := nodedb.Stmts["update-value-file"]
	_, err := stmt.Exec(
		valueFile.creationTime,
		closeTime,
		valueFile.bytesWritten,
		md5Digest,
		valueFile.sequencesWritten,
		valueFile.minSegmentID,
		valueFile.maxSegmentID,
		len(collectionIDs),
		collectionIDLiteral,
		valueFile.valueFileID)
	return err
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}
