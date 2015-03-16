package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"

	"fog"
	"writermsg"

	"datawriter/logger"
	"datawriter/nodedb"
	"datawriter/reply"
	"datawriter/types"
	"datawriter/writer"
)

const (
	messageChanCapacity = 100
)

// message handler takes a message and returns a reply
type messageHandler func(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage)

func NewMessageHandler(nodeIDMap map[string]uint32) chan<- types.Message {
	var nimbusioWriter writer.NimbusioWriter
	var replyChan chan<- *reply.ReplyMessage
	var err error

	messageChan := make(chan types.Message, messageChanCapacity)

	if err = nodedb.Initialize(); err != nil {
		fog.Critical("NewMessageHandler: nodedb.Initialize failed %s", err)
	}

	if nimbusioWriter, err = writer.NewNimbusioWriter(nodeIDMap); err != nil {
		fog.Critical("NewMessageHandler: NewNimbusioWriter() failed %s", err)
	}

	replyChan = reply.NewReplyHandler()

	dispatchTable := map[string]messageHandler{
		"archive-key-entire":       handleArchiveKeyEntire,
		"archive-key-start":        handleArchiveKeyStart,
		"archive-key-next":         handleArchiveKeyNext,
		"archive-key-final":        handleArchiveKeyFinal,
		"archive-key-cancel":       handleArchiveKeyCancel,
		"destroy-key":              handleDestroyKey,
		"start-conjoined-archive":  handleStartConjoinedArchive,
		"abort-conjoined-archive":  handleAbortConjoinedArchive,
		"finish-conjoined-archive": handleFinishConjoinedArchive}

	go func() {
		for message := range messageChan {
			handler, ok := dispatchTable[message.Type]
			if !ok {
				fog.Error("Unknown message type %s %s",
					message.Type, message.Marshalled)
				continue
			}
			go handler(message, nimbusioWriter, replyChan)
		}
	}()

	return messageChan
}

func handleArchiveKeyEntire(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var archiveKeyEntire writermsg.ArchiveKeyEntire
	var md5Digest []byte
	var err error
	var valueFileID uint32

	archiveKeyEntire, err = writermsg.UnmarshalArchiveKeyEntire(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalArchiveKeyEntire failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("archive-key-final", message.ID,
		archiveKeyEntire.UserRequestID, archiveKeyEntire.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyEntire.UserRequestID, archiveKeyEntire.UnifiedID,
		archiveKeyEntire.ConjoinedPart, archiveKeyEntire.SegmentNum, archiveKeyEntire.Key)
	lgr.Info("archive-key-entire")

	if archiveKeyEntire.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", archiveKeyEntire.SegmentSize,
			len(message.Data))
		replyMessage.Error("size-mismatch", "segment size does not match expected value")
		replyChan <- replyMessage
		return
	}

	md5Digest, err = base64.StdEncoding.DecodeString(archiveKeyEntire.EncodedSegmentMD5Digest)
	if err != nil {
		lgr.Error("base64.StdEncoding.DecodeString %s", err)
		replyMessage.Error("base64", "base64.StdEncoding.DecodeString")
		replyChan <- replyMessage
		return
	}

	if !MD5DigestMatches(message.Data, md5Digest) {
		lgr.Error("md5 mismatch")
		replyMessage.Error("md5-mismatch", "segment md5 does not match expected value")
		replyChan <- replyMessage
		return
	}

	err = nimbusioWriter.StartSegment(archiveKeyEntire.UserRequestID, archiveKeyEntire.Segment,
		archiveKeyEntire.NodeNames)
	if err != nil {
		lgr.Error("StartSegment: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	valueFileID, err = nimbusioWriter.StoreSequence(
		archiveKeyEntire.UserRequestID,
		archiveKeyEntire.Segment,
		archiveKeyEntire.Sequence, message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	metaData := writermsg.GetMetaFromJSON(message.Marshalled)

	err = nimbusioWriter.FinishSegment(archiveKeyEntire.UserRequestID,
		archiveKeyEntire.Segment,
		archiveKeyEntire.File,
		metaData,
		valueFileID)
	if err != nil {
		lgr.Error("FinishSegment: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func handleArchiveKeyStart(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var archiveKeyStart writermsg.ArchiveKeyStart
	var md5Digest []byte
	var err error

	archiveKeyStart, err = writermsg.UnmarshalArchiveKeyStart(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalArchiveKeyStart failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("archive-key-final", message.ID,
		archiveKeyStart.UserRequestID, archiveKeyStart.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyStart.UserRequestID, archiveKeyStart.UnifiedID,
		archiveKeyStart.ConjoinedPart, archiveKeyStart.SegmentNum, archiveKeyStart.Key)
	lgr.Info("archive-key-Start")

	if archiveKeyStart.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", archiveKeyStart.SegmentSize,
			len(message.Data))
		replyMessage.Error("size-mismatch", "segment size does not match expected value")
		replyChan <- replyMessage
		return
	}

	md5Digest, err = base64.StdEncoding.DecodeString(archiveKeyStart.EncodedSegmentMD5Digest)
	if err != nil {
		lgr.Error("base64.StdEncoding.DecodeString %s", err)
		replyMessage.Error("base64", "base64.StdEncoding.DecodeString")
		replyChan <- replyMessage
		return
	}

	if !MD5DigestMatches(message.Data, md5Digest) {
		lgr.Error("md5 mismatch")
		replyMessage.Error("md5-mismatch", "segment md5 does not match expected value")
		replyChan <- replyMessage
		return
	}

	err = nimbusioWriter.StartSegment(archiveKeyStart.UserRequestID,
		archiveKeyStart.Segment,
		archiveKeyStart.NodeNames)
	if err != nil {
		lgr.Error("StartSegment: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	_, err = nimbusioWriter.StoreSequence(archiveKeyStart.UserRequestID,
		archiveKeyStart.Segment,
		archiveKeyStart.Sequence, message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func handleArchiveKeyNext(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var archiveKeyNext writermsg.ArchiveKeyNext
	var md5Digest []byte
	var err error

	archiveKeyNext, err = writermsg.UnmarshalArchiveKeyNext(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalArchiveKeyNext failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("archive-key-final", message.ID,
		archiveKeyNext.UserRequestID, archiveKeyNext.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyNext.UserRequestID, archiveKeyNext.UnifiedID,
		archiveKeyNext.ConjoinedPart, archiveKeyNext.SegmentNum, archiveKeyNext.Key)
	lgr.Info("archive-key-Next")

	if archiveKeyNext.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", archiveKeyNext.SegmentSize,
			len(message.Data))
		replyMessage.Error("size-mismatch", "segment size does not match expected value")
		replyChan <- replyMessage
		return
	}

	md5Digest, err = base64.StdEncoding.DecodeString(archiveKeyNext.EncodedSegmentMD5Digest)
	if err != nil {
		lgr.Error("base64.StdEncoding.DecodeString %s", err)
		replyMessage.Error("base64", "base64.StdEncoding.DecodeString")
		replyChan <- replyMessage
		return
	}

	if !MD5DigestMatches(message.Data, md5Digest) {
		lgr.Error("md5 mismatch")
		replyMessage.Error("md5-mismatch", "segment md5 does not match expected value")
		replyChan <- replyMessage
		return
	}

	_, err = nimbusioWriter.StoreSequence(archiveKeyNext.UserRequestID,
		archiveKeyNext.Segment,
		archiveKeyNext.Sequence, message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func handleArchiveKeyFinal(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var archiveKeyFinal writermsg.ArchiveKeyFinal
	var md5Digest []byte
	var err error
	var valueFileID uint32

	archiveKeyFinal, err = writermsg.UnmarshalArchiveKeyFinal(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalArchiveKeyFinal failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("archive-key-final", message.ID,
		archiveKeyFinal.UserRequestID, archiveKeyFinal.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyFinal.UserRequestID, archiveKeyFinal.UnifiedID,
		archiveKeyFinal.ConjoinedPart, archiveKeyFinal.SegmentNum, archiveKeyFinal.Key)
	lgr.Info("archive-key-Final")

	if archiveKeyFinal.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", archiveKeyFinal.SegmentSize,
			len(message.Data))
		replyMessage.Error("size-mismatch", "segment size does not match expected value")
		replyChan <- replyMessage
		return
	}

	md5Digest, err = base64.StdEncoding.DecodeString(archiveKeyFinal.EncodedSegmentMD5Digest)
	if err != nil {
		lgr.Error("base64.StdEncoding.DecodeString %s", err)
		replyMessage.Error("base64", "base64.StdEncoding.DecodeString")
		replyChan <- replyMessage
		return
	}

	if !MD5DigestMatches(message.Data, md5Digest) {
		lgr.Error("md5 mismatch")
		replyMessage.Error("md5-mismatch", "segment md5 does not match expected value")
		replyChan <- replyMessage
		return
	}

	valueFileID, err = nimbusioWriter.StoreSequence(
		archiveKeyFinal.UserRequestID,
		archiveKeyFinal.Segment,
		archiveKeyFinal.Sequence, message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	metaData := writermsg.GetMetaFromJSON(message.Marshalled)

	err = nimbusioWriter.FinishSegment(archiveKeyFinal.UserRequestID,
		archiveKeyFinal.Segment,
		archiveKeyFinal.File,
		metaData,
		valueFileID)
	if err != nil {
		lgr.Error("FinishSegment: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func handleArchiveKeyCancel(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var archiveKeyCancel writermsg.ArchiveKeyCancel
	var err error

	archiveKeyCancel, err = writermsg.UnmarshalArchiveKeyCancel(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalArchiveKeyCancel failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("archive-key-cancel", message.ID,
		archiveKeyCancel.UserRequestID, archiveKeyCancel.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyCancel.UserRequestID, archiveKeyCancel.UnifiedID,
		archiveKeyCancel.ConjoinedPart, archiveKeyCancel.SegmentNum, "")
	lgr.Info("archive-key-cancel")

	if err = nimbusioWriter.CancelSegment(archiveKeyCancel); err != nil {
		lgr.Error("CancelSegment: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func handleDestroyKey(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var destroyKey writermsg.DestroyKey
	var err error

	destroyKey, err = writermsg.UnmarshalDestroyKey(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalDestroyKey failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("destroy-key", message.ID,
		destroyKey.UserRequestID, destroyKey.ReturnAddress)

	lgr := logger.NewLogger(destroyKey.UserRequestID, destroyKey.UnifiedID,
		destroyKey.ConjoinedPart, destroyKey.SegmentNum, destroyKey.Key)
	lgr.Info("archive-key-cancel")

	if err = nimbusioWriter.DestroyKey(destroyKey); err != nil {
		lgr.Error("DestroyKey: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func handleStartConjoinedArchive(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var conjoined writermsg.Conjoined
	var err error

	conjoined, err = writermsg.UnmarshalConjoined(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalConjoined failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("start-conjoined-archive", message.ID,
		conjoined.UserRequestID, conjoined.ReturnAddress)

	lgr := logger.NewLogger(conjoined.UserRequestID, conjoined.UnifiedID,
		0, 0, conjoined.Key)
	lgr.Info("start-conjoined-archive (%d)", conjoined.CollectionID)

	if err = nimbusioWriter.StartConjoinedArchive(conjoined); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func handleAbortConjoinedArchive(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var conjoined writermsg.Conjoined
	var err error

	conjoined, err = writermsg.UnmarshalConjoined(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalConjoined failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("abort-conjoined-archive", message.ID,
		conjoined.UserRequestID, conjoined.ReturnAddress)

	lgr := logger.NewLogger(conjoined.UserRequestID, conjoined.UnifiedID,
		0, 0, conjoined.Key)
	lgr.Info("abort-conjoined-archive (%d)", conjoined.CollectionID)

	if err = nimbusioWriter.AbortConjoinedArchive(conjoined); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func handleFinishConjoinedArchive(message types.Message,
	nimbusioWriter writer.NimbusioWriter,
	replyChan chan<- *reply.ReplyMessage) {

	var conjoined writermsg.Conjoined
	var err error

	conjoined, err = writermsg.UnmarshalConjoined(message.Marshalled)
	if err != nil {
		fog.Error("UnmarshalConjoined failed %s", err)
		return
	}

	replyMessage := reply.NewReplyMessage("finish-conjoined-archive", message.ID,
		conjoined.UserRequestID, conjoined.ReturnAddress)

	lgr := logger.NewLogger(conjoined.UserRequestID, conjoined.UnifiedID,
		0, 0, conjoined.Key)
	lgr.Info("finish-conjoined-archive (%d)", conjoined.CollectionID)

	if err = nimbusioWriter.FinishConjoinedArchive(conjoined); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		replyMessage.Error("error", err.Error())
		replyChan <- replyMessage
		return
	}

	replyMessage.Success()
	replyChan <- replyMessage
	return
}

func MD5DigestMatches(data []byte, md5Digest []byte) bool {
	hasher := md5.New()
	hasher.Write(data)
	dataMd5Digest := hasher.Sum(nil)

	return bytes.Equal(dataMd5Digest, md5Digest)
}
