package writer

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"time"

	"fog"
	"tools"

	"datawriter/msg"
	"datawriter/nodedb"
)

type NimbusioWriter interface {

	// StartSegment initializes a new segment and prepares to receive data
	// for it
	StartSegment(userRequestID string, segment msg.Segment, nodeNames msg.NodeNames) error

	// StoreSequence stores data for  an initialized segment
	// return the ID of the value file written to
	StoreSequence(userRequestID string, segment msg.Segment,
		sequence msg.Sequence, data []byte) (uint32, error)

	// CancelSegment stops processing the segment
	CancelSegment(cancel msg.ArchiveKeyCancel) error

	// FinishSegment finishes storing the segment
	FinishSegment(userRequestID string, segment msg.Segment, file msg.File,
		metaData []msg.MetaPair, valueFileID uint32) error

	// DestroyKey makes a key inaccessible
	DestroyKey(destroyKey msg.DestroyKey) error

	// StartConjoinedArchive begins a conjoined archive
	StartConjoinedArchive(conjoined msg.Conjoined) error

	// AbortConjoinedArchive cancels conjoined archive
	AbortConjoinedArchive(conjoined msg.Conjoined) error

	// FinishConjoinedArchive completes a conjoined archive
	FinishConjoinedArchive(conjoined msg.Conjoined) error
}

const (
	writerChanCapacity = 1000
	syncInterval       = time.Second * 1
)

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

type nimbusioWriterChan chan<- interface{}

type writerState struct {
	// map data contained in messages onto our internal segment id
	NodeIDMap        map[string]uint32
	SegmentMap       map[segmentKey]segmentMapEntry
	FileSpaceInfo    tools.FileSpaceInfo
	ValueFile        OutputValueFile
	MaxValueFileSize uint64
	WriterChan       nimbusioWriterChan
	SyncTimer        *time.Timer
	WaitSyncRequests []requestFinishSegment
}

type requestSync struct{}

type requestStartSegment struct {
	UserRequestID string
	Segment       msg.Segment
	NodeNames     msg.NodeNames
	resultChan    chan<- error
}

type storeSequenceResult struct {
	ValueFileID uint32
	Err         error
}
type requestStoreSequence struct {
	UserRequestID string
	Segment       msg.Segment
	Sequence      msg.Sequence
	Data          []byte
	resultChan    chan<- storeSequenceResult
}

type requestCancelSegment struct {
	Cancel     msg.ArchiveKeyCancel
	resultChan chan<- error
}

type requestWaitSyncForFinishSegment struct {
	UserRequestID string
	Segment       msg.Segment
	File          msg.File
	MetaData      []msg.MetaPair
	ValueFileID   uint32
	resultChan    chan<- error
}

type requestFinishSegment struct {
	UserRequestID string
	Segment       msg.Segment
	File          msg.File
	MetaData      []msg.MetaPair
	resultChan    chan<- error
}

type requestDestroyKey struct {
	DestroyKey msg.DestroyKey
	resultChan chan<- error
}

type requestStartConjoinedArchive struct {
	Conjoined  msg.Conjoined
	resultChan chan<- error
}

type requestAbortConjoinedArchive struct {
	Conjoined  msg.Conjoined
	resultChan chan<- error
}

type requestFinishConjoinedArchive struct {
	Conjoined  msg.Conjoined
	resultChan chan<- error
}

// NewNimbusioWriter returns an entity that implements the NimbusioWriter interface
func NewNimbusioWriter() (NimbusioWriter, error) {
	var err error
	var state writerState
	state.SegmentMap = make(map[segmentKey]segmentMapEntry)

	if state.NodeIDMap, err = tools.GetNodeIDMap(); err != nil {
		return nil, fmt.Errorf("tools.GetNodeIDMap() failed %s", err)
	}

	maxValueFileSizeStr := os.Getenv("NIMBUS_IO_MAX_VALUE_FILE_SIZE")
	if maxValueFileSizeStr == "" {
		state.MaxValueFileSize = uint64(1024 * 1024 * 1024)
	} else {
		var intSize int
		intSize, err = strconv.Atoi(maxValueFileSizeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid NIMBUS_IO_MAX_VALUE_FILE_SIZE '%s'",
				maxValueFileSizeStr)
		}
		state.MaxValueFileSize = uint64(intSize)
	}

	if state.FileSpaceInfo, err = tools.NewFileSpaceInfo(nodedb.NodeDB); err != nil {
		return nil, err
	}

	if state.ValueFile, err = NewOutputValueFile(state.FileSpaceInfo); err != nil {
		return nil, err
	}

	writerChan := make(chan interface{}, writerChanCapacity)
	state.WriterChan = nimbusioWriterChan(writerChan)

	startSyncTimer(&state)

	go func() {
		for item := range writerChan {
			switch request := item.(type) {
			case requestSync:
				handleSync(&state)
			case requestStartSegment:
				handleStartSegment(&state, request)
			case requestStoreSequence:
				handleStoreSequence(&state, request)
			case requestCancelSegment:
				handleCancelSegment(&state, request)
			case requestWaitSyncForFinishSegment:
				handleWaitSyncForFinishSegment(&state, request)
			case requestFinishSegment:
				handleFinishSegment(&state, request)
			case requestDestroyKey:
				handleDestroyKey(&state, request)
			case requestStartConjoinedArchive:
				handleStartConjoinedArchive(&state, request)
			case requestAbortConjoinedArchive:
				handleAbortConjoinedArchive(&state, request)
			case requestFinishConjoinedArchive:
				handleFinishConjoinedArchive(&state, request)
			default:
				fog.Error("unknown request type %T", item)
			}
		}
	}()

	return state.WriterChan, nil
}

func startSyncTimer(state *writerState) {
	if state.SyncTimer != nil {
		state.SyncTimer.Stop()
	}

	state.SyncTimer = time.AfterFunc(
		syncInterval,
		func() { state.WriterChan <- requestSync{} })
}

func handleSync(state *writerState) {
	if state.SyncTimer != nil {
		state.SyncTimer.Stop()
	}
	state.SyncTimer = nil
	if err := state.ValueFile.Sync(); err != nil {
		fog.Error("sync failed %s", err)
	}

	for _, request := range state.WaitSyncRequests {
		state.WriterChan <- request
	}
	state.WaitSyncRequests = nil

	startSyncTimer(state)
}

func handleStartSegment(state *writerState, request requestStartSegment) {
	userRequestID := request.UserRequestID
	segment := request.Segment
	nodeNames := request.NodeNames

	var entry segmentMapEntry
	var err error
	var sourceNodeID uint32
	var handoffNodeID uint32
	var ok bool
	var timestamp time.Time

	fog.Debug("%s StartSegment", userRequestID)

	if sourceNodeID, ok = state.NodeIDMap[nodeNames.SourceNodeName]; !ok {
		request.resultChan <- fmt.Errorf("unknown source node %s", nodeNames.SourceNodeName)
		return
	}

	if timestamp, err = tools.ParseTimestampRepr(segment.TimestampRepr); err != nil {
		request.resultChan <- fmt.Errorf("unable to parse timestamp %s", err)
		return
	}

	if nodeNames.HandoffNodeName != "" {
		if handoffNodeID, ok = state.NodeIDMap[nodeNames.HandoffNodeName]; !ok {
			request.resultChan <- fmt.Errorf("unknown handoff node %s", nodeNames.HandoffNodeName)
			return
		}

		stmt := nodedb.Stmts["new-segment-for-handoff"]
		row := stmt.QueryRow(
			segment.CollectionID,
			segment.Key,
			segment.UnifiedID,
			timestamp,
			segment.SegmentNum,
			segment.ConjoinedPart,
			sourceNodeID,
			handoffNodeID)
		if err = row.Scan(&entry.SegmentID); err != nil {
			request.resultChan <- err
			return
		}
	} else {
		stmt := nodedb.Stmts["new-segment"]
		row := stmt.QueryRow(
			segment.CollectionID,
			segment.Key,
			segment.UnifiedID,
			timestamp,
			segment.SegmentNum,
			segment.ConjoinedPart,
			sourceNodeID)
		if err = row.Scan(&entry.SegmentID); err != nil {
			request.resultChan <- err
			return
		}
	}
	entry.LastActionTime = tools.Timestamp()

	key := segmentKey{segment.UnifiedID, segment.ConjoinedPart,
		segment.SegmentNum}

	state.SegmentMap[key] = entry

	request.resultChan <- nil
}

func handleStoreSequence(state *writerState, request requestStoreSequence) {
	userRequestID := request.UserRequestID
	segment := request.Segment
	sequence := request.Sequence
	data := request.Data
	var err error
	var md5Digest []byte
	var offset uint64

	fog.Debug("%s StoreSequence #%d", userRequestID, sequence.SequenceNum)

	if state.ValueFile.Size()+sequence.SegmentSize >= state.MaxValueFileSize {
		fog.Info("value file full")

		if state.SyncTimer != nil {
			state.SyncTimer.Stop()
		}
		state.SyncTimer = nil

		if err = state.ValueFile.Close(); err != nil {
			request.resultChan <- storeSequenceResult{Err: fmt.Errorf("error closing value file %s", err)}
			return
		}

		if state.ValueFile, err = NewOutputValueFile(state.FileSpaceInfo); err != nil {
			request.resultChan <- storeSequenceResult{Err: fmt.Errorf("error opening value file %s", err)}
			return
		}

		startSyncTimer(state)
	}

	md5Digest, err = base64.StdEncoding.DecodeString(sequence.EncodedSegmentMD5Digest)
	if err != nil {
		request.resultChan <- storeSequenceResult{Err: err}
		return
	}

	key := segmentKey{segment.UnifiedID, segment.ConjoinedPart,
		segment.SegmentNum}
	entry, ok := state.SegmentMap[key]
	if !ok {
		request.resultChan <- storeSequenceResult{Err: fmt.Errorf("StoreSequence unknown segment %s", key)}
		return
	}

	offset, err = state.ValueFile.Store(segment.CollectionID, entry.SegmentID,
		data)
	if err != nil {
		request.resultChan <- storeSequenceResult{Err: fmt.Errorf("ValueFile.Store %s", err)}
		return
	}

	stmt := nodedb.Stmts["new-segment-sequence"]
	_, err = stmt.Exec(
		segment.CollectionID,
		entry.SegmentID,
		sequence.ZfecPaddingSize,
		state.ValueFile.ID(),
		sequence.SequenceNum,
		offset,
		sequence.SegmentSize,
		md5Digest,
		sequence.SegmentAdler32)
	if err != nil {
		request.resultChan <- storeSequenceResult{Err: fmt.Errorf("new-segment-sequence %s", err)}
	}

	entry.LastActionTime = tools.Timestamp()
	state.SegmentMap[key] = entry

	request.resultChan <- storeSequenceResult{ValueFileID: state.ValueFile.ID()}
}

// CancelSegment stops storing the segment
func handleCancelSegment(state *writerState, request requestCancelSegment) {
	cancel := request.Cancel
	var err error

	fog.Debug("%s CancelSegment", cancel.UserRequestID)

	key := segmentKey{cancel.UnifiedID, cancel.ConjoinedPart,
		cancel.SegmentNum}
	delete(state.SegmentMap, key)

	stmt := nodedb.Stmts["cancel-segment"]
	_, err = stmt.Exec(
		cancel.UnifiedID,
		cancel.ConjoinedPart,
		cancel.SegmentNum)

	if err != nil {
		request.resultChan <- fmt.Errorf("cancel-segment %s", err)
		return
	}

	request.resultChan <- nil
}

// FinishSegment finishes storing the segment
func handleWaitSyncForFinishSegment(state *writerState, request requestWaitSyncForFinishSegment) {

	finishRequest := requestFinishSegment{
		UserRequestID: request.UserRequestID,
		Segment:       request.Segment,
		File:          request.File,
		MetaData:      request.MetaData,
		resultChan:    request.resultChan}

	// We now want to wait until the value file is synced to disk, but
	// only if the current value file is the last one we wrote to
	if request.ValueFileID != state.ValueFile.ID() {
		fog.Warn("%s not waiting for value file sync (%d)", request.UserRequestID,
			request.ValueFileID)
		state.WriterChan <- finishRequest
		return
	}

	state.WaitSyncRequests = append(state.WaitSyncRequests, finishRequest)
}

// FinishSegment finishes storing the segment
func handleFinishSegment(state *writerState, request requestFinishSegment) {
	userRequestID := request.UserRequestID
	segment := request.Segment
	file := request.File
	metaData := request.MetaData
	var err error
	var md5Digest []byte
	var timestamp time.Time

	fog.Debug("%s FinishSegment", userRequestID)

	key := segmentKey{segment.UnifiedID, segment.ConjoinedPart,
		segment.SegmentNum}
	entry, ok := state.SegmentMap[key]
	if !ok {
		request.resultChan <- fmt.Errorf("FinishSegment unknown segment %s", key)
		return
	}

	delete(state.SegmentMap, key)

	md5Digest, err = base64.StdEncoding.DecodeString(file.EncodedFileMD5Digest)
	if err != nil {
		request.resultChan <- err
		return
	}

	if timestamp, err = tools.ParseTimestampRepr(segment.TimestampRepr); err != nil {
		request.resultChan <- fmt.Errorf("unable to parse timestamp %s", err)
		return
	}

	stmt := nodedb.Stmts["finish-segment"]
	_, err = stmt.Exec(
		file.FileSize,
		file.FileAdler32,
		md5Digest,
		entry.SegmentID)

	if err != nil {
		request.resultChan <- fmt.Errorf("finish-segment %s", err)
		return
	}

	for _, metaEntry := range metaData {
		stmt := nodedb.Stmts["new-meta-data"]
		_, err = stmt.Exec(
			segment.CollectionID,
			entry.SegmentID,
			metaEntry.Key,
			metaEntry.Value,
			timestamp)

		if err != nil {
			request.resultChan <- fmt.Errorf("new-meta-data %s", err)
			return
		}
	}

	request.resultChan <- nil
}

// DestroyKey makes a key inaccessible
func handleDestroyKey(state *writerState, request requestDestroyKey) {
	destroyKey := request.DestroyKey
	var err error
	var ok bool
	var timestamp time.Time
	var sourceNodeID uint32
	var handoffNodeID uint32

	fog.Debug("DestroyKey (%d)", destroyKey.UnifiedIDToDestroy)

	if sourceNodeID, ok = state.NodeIDMap[destroyKey.SourceNodeName]; !ok {
		request.resultChan <- fmt.Errorf("unknown source node %s", destroyKey.SourceNodeName)
		return
	}

	if timestamp, err = tools.ParseTimestampRepr(destroyKey.TimestampRepr); err != nil {
		request.resultChan <- fmt.Errorf("unable to parse timestamp %s", err)
		return
	}

	if destroyKey.UnifiedIDToDestroy > 0 {
		if destroyKey.HandoffNodeName != "" {
			if handoffNodeID, ok = state.NodeIDMap[destroyKey.HandoffNodeName]; !ok {
				request.resultChan <- fmt.Errorf("unknown handoff node %s", destroyKey.HandoffNodeName)
				return
			}
			stmt := nodedb.Stmts["new-tombstone-for-unified-id-for-handoff"]
			_, err = stmt.Exec(
				destroyKey.CollectionID,
				destroyKey.Key,
				destroyKey.UnifiedID,
				timestamp,
				destroyKey.SegmentNum,
				destroyKey.UnifiedIDToDestroy,
				sourceNodeID,
				handoffNodeID)

			if err != nil {
				request.resultChan <- fmt.Errorf("new-tombstone-for-unified-id-for-handoff %d %s",
					destroyKey.UnifiedIDToDestroy, err)
				return
			}
		} else {
			stmt := nodedb.Stmts["new-tombstone-for-unified-id"]
			_, err = stmt.Exec(
				destroyKey.CollectionID,
				destroyKey.Key,
				destroyKey.UnifiedID,
				timestamp,
				destroyKey.SegmentNum,
				destroyKey.UnifiedIDToDestroy,
				sourceNodeID)

			if err != nil {
				request.resultChan <- fmt.Errorf("new-tombstone-for-unified-id %d %s",
					destroyKey.UnifiedIDToDestroy, err)
				return
			}
		}

		stmt := nodedb.Stmts["delete-conjoined-for-unified-id"]
		_, err = stmt.Exec(
			timestamp,
			destroyKey.CollectionID,
			destroyKey.Key,
			destroyKey.UnifiedIDToDestroy)

		if err != nil {
			request.resultChan <- fmt.Errorf("delete-conjoined-for-unified-id %d %s",
				destroyKey.UnifiedIDToDestroy, err)
			return
		}
	} else {
		if destroyKey.HandoffNodeName != "" {
			if handoffNodeID, ok = state.NodeIDMap[destroyKey.HandoffNodeName]; !ok {
				request.resultChan <- fmt.Errorf("unknown handoff node %s", destroyKey.HandoffNodeName)
				return
			}
			stmt := nodedb.Stmts["new-tombstone-for-handoff"]
			_, err = stmt.Exec(
				destroyKey.CollectionID,
				destroyKey.Key,
				destroyKey.UnifiedID,
				timestamp,
				destroyKey.SegmentNum,
				sourceNodeID,
				handoffNodeID)

			if err != nil {
				request.resultChan <- fmt.Errorf("new-tombstone-for-handoff %s", err)
				return
			}
		} else {
			stmt := nodedb.Stmts["new-tombstone"]
			_, err = stmt.Exec(
				destroyKey.CollectionID,
				destroyKey.Key,
				destroyKey.UnifiedID,
				timestamp,
				destroyKey.SegmentNum,
				sourceNodeID)

			if err != nil {
				request.resultChan <- fmt.Errorf("new-tombstone %s", err)
				return
			}
		}

		stmt := nodedb.Stmts["delete-conjoined"]
		_, err = stmt.Exec(
			timestamp,
			destroyKey.CollectionID,
			destroyKey.Key,
			destroyKey.UnifiedID)

		if err != nil {
			request.resultChan <- fmt.Errorf("delete-conjoined %s", err)
			return
		}
	}

	request.resultChan <- nil
}

// StartConjoinedArchive begins a conjoined archive
func handleStartConjoinedArchive(state *writerState, request requestStartConjoinedArchive) {
	conjoined := request.Conjoined
	var err error
	var ok bool
	var handoffNodeID uint32
	var timestamp time.Time

	fog.Debug("%s StartConjoinedArchive %s", conjoined.UserRequestID, conjoined)

	if timestamp, err = tools.ParseTimestampRepr(conjoined.TimestampRepr); err != nil {
		request.resultChan <- fmt.Errorf("unable to parse timestamp %s", err)
		return
	}

	if conjoined.HandoffNodeName != "" {
		if handoffNodeID, ok = state.NodeIDMap[conjoined.HandoffNodeName]; !ok {
			request.resultChan <- fmt.Errorf("unknown handoff node %s", conjoined.HandoffNodeName)
			return
		}
		stmt := nodedb.Stmts["start-conjoined-for-handoff"]
		_, err = stmt.Exec(
			conjoined.CollectionID,
			conjoined.Key,
			conjoined.UnifiedID,
			timestamp,
			handoffNodeID)

		if err != nil {
			request.resultChan <- fmt.Errorf("start-conjoined-for-handoff %s", err)
			return
		}
	} else {
		stmt := nodedb.Stmts["start-conjoined"]
		_, err = stmt.Exec(
			conjoined.CollectionID,
			conjoined.Key,
			conjoined.UnifiedID,
			timestamp)

		if err != nil {
			request.resultChan <- fmt.Errorf("start-conjoined %s", err)
			return
		}
	}

	request.resultChan <- nil
}

// AbortConjoinedArchive cancels conjoined archive
func handleAbortConjoinedArchive(state *writerState, request requestAbortConjoinedArchive) {
	conjoined := request.Conjoined
	var err error
	var ok bool
	var handoffNodeID uint32
	var timestamp time.Time

	fog.Debug("%s AbortConjoinedArchive %s", conjoined.UserRequestID, conjoined)

	if timestamp, err = tools.ParseTimestampRepr(conjoined.TimestampRepr); err != nil {
		request.resultChan <- fmt.Errorf("unable to parse timestamp %s", err)
		return
	}

	if conjoined.HandoffNodeName != "" {
		if handoffNodeID, ok = state.NodeIDMap[conjoined.HandoffNodeName]; !ok {
			request.resultChan <- fmt.Errorf("unknown handoff node %s", conjoined.HandoffNodeName)
			return
		}
		stmt := nodedb.Stmts["abort-conjoined-for-handoff"]
		_, err = stmt.Exec(
			timestamp,
			conjoined.CollectionID,
			conjoined.Key,
			conjoined.UnifiedID,
			handoffNodeID)

		if err != nil {
			request.resultChan <- fmt.Errorf("abort-conjoined-for-handoff %s", err)
			return
		}
	} else {

		stmt := nodedb.Stmts["abort-conjoined"]
		_, err = stmt.Exec(
			timestamp,
			conjoined.CollectionID,
			conjoined.Key,
			conjoined.UnifiedID)

		if err != nil {
			request.resultChan <- fmt.Errorf("abort-conjoined %s", err)
			return
		}
	}

	request.resultChan <- nil
}

// FinishConjoinedArchive completes a conjoined archive
func handleFinishConjoinedArchive(state *writerState, request requestFinishConjoinedArchive) {
	conjoined := request.Conjoined
	var err error
	var ok bool
	var handoffNodeID uint32
	var timestamp time.Time

	fog.Debug("%s FinishConjoinedArchive %s", conjoined.UserRequestID, conjoined)

	if timestamp, err = tools.ParseTimestampRepr(conjoined.TimestampRepr); err != nil {
		request.resultChan <- fmt.Errorf("unable to parse timestamp %s", err)
		return
	}

	if conjoined.HandoffNodeName != "" {
		if handoffNodeID, ok = state.NodeIDMap[conjoined.HandoffNodeName]; !ok {
			request.resultChan <- fmt.Errorf("unknown handoff node %s", conjoined.HandoffNodeName)
		}
		stmt := nodedb.Stmts["finish-conjoined-for-handoff"]
		_, err = stmt.Exec(
			timestamp,
			conjoined.CollectionID,
			conjoined.Key,
			conjoined.UnifiedID,
			handoffNodeID)

		if err != nil {
			request.resultChan <- fmt.Errorf("finish-conjoined-for-handoff %s", err)
			return
		}
	} else {

		stmt := nodedb.Stmts["finish-conjoined"]
		_, err = stmt.Exec(
			timestamp,
			conjoined.CollectionID,
			conjoined.Key,
			conjoined.UnifiedID)

		if err != nil {
			request.resultChan <- fmt.Errorf("finish-conjoined %s", err)
			return
		}

	}

	request.resultChan <- nil
}

func (writerChan nimbusioWriterChan) StartSegment(userRequestID string,
	segment msg.Segment, nodeNames msg.NodeNames) error {

	resultChan := make(chan error)
	request := requestStartSegment{
		UserRequestID: userRequestID,
		Segment:       segment,
		NodeNames:     nodeNames,
		resultChan:    resultChan}

	writerChan <- request
	return <-resultChan
}

func (writerChan nimbusioWriterChan) StoreSequence(userRequestID string,
	segment msg.Segment,
	sequence msg.Sequence, data []byte) (uint32, error) {
	resultChan := make(chan storeSequenceResult)
	request := requestStoreSequence{
		UserRequestID: userRequestID,
		Segment:       segment,
		Sequence:      sequence,
		Data:          data,
		resultChan:    resultChan}

	writerChan <- request
	result := <-resultChan
	return result.ValueFileID, result.Err
}

// CancelSegment stops storing the segment
func (writerChan nimbusioWriterChan) CancelSegment(cancel msg.ArchiveKeyCancel) error {
	resultChan := make(chan error)
	request := requestCancelSegment{
		Cancel:     cancel,
		resultChan: resultChan}

	writerChan <- request
	return <-resultChan
}

// FinishSegment finishes storing the segment
func (writerChan nimbusioWriterChan) FinishSegment(userRequestID string,
	segment msg.Segment, file msg.File, metaData []msg.MetaPair,
	valueFileID uint32) error {

	resultChan := make(chan error)
	request := requestWaitSyncForFinishSegment{
		UserRequestID: userRequestID,
		Segment:       segment,
		File:          file,
		MetaData:      metaData,
		ValueFileID:   valueFileID,
		resultChan:    resultChan}

	writerChan <- request
	return <-resultChan
}

// DestroyKey makes a key inaccessible
func (writerChan nimbusioWriterChan) DestroyKey(destroyKey msg.DestroyKey) error {
	resultChan := make(chan error)
	request := requestDestroyKey{
		DestroyKey: destroyKey,
		resultChan: resultChan}

	writerChan <- request
	return <-resultChan
}

// StartConjoinedArchive begins a conjoined archive
func (writerChan nimbusioWriterChan) StartConjoinedArchive(conjoined msg.Conjoined) error {
	resultChan := make(chan error)
	request := requestStartConjoinedArchive{
		Conjoined:  conjoined,
		resultChan: resultChan}

	writerChan <- request
	return <-resultChan
}

// AbortConjoinedArchive cancels conjoined archive
func (writerChan nimbusioWriterChan) AbortConjoinedArchive(conjoined msg.Conjoined) error {
	resultChan := make(chan error)
	request := requestAbortConjoinedArchive{
		Conjoined:  conjoined,
		resultChan: resultChan}

	writerChan <- request
	return <-resultChan
}

// FinishConjoinedArchive completes a conjoined archive
func (writerChan nimbusioWriterChan) FinishConjoinedArchive(conjoined msg.Conjoined) error {
	resultChan := make(chan error)
	request := requestFinishConjoinedArchive{
		Conjoined:  conjoined,
		resultChan: resultChan}

	writerChan <- request
	return <-resultChan
}
