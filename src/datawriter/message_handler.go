package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/pebbe/zmq4"

	"fog"

	"datawriter/logger"
	"datawriter/msg"
	"datawriter/nodedb"
	"datawriter/types"
	"datawriter/writer"
)

const (
	messageChanCapacity = 100
	pushSocketSendHWM   = 10
)

type Reply struct {
	ClientAddress string
	MessageMap    map[string]interface{}
}

// message handler takes a message and returns a reply
type messageHandler func(message types.Message) (Reply, error)

var (
	nimbusioWriter writer.NimbusioWriter
)

func NewMessageHandler() chan<- types.Message {
	var err error

	messageChan := make(chan types.Message, messageChanCapacity)

	if err = nodedb.Initialize(); err != nil {
		fog.Critical("NewMessageHandler: nodedb.Initialize failed %s", err)
	}

	if nimbusioWriter, err = writer.NewNimbusioWriter(); err != nil {
		fog.Critical("NewMessageHandler: NewNimbusioWriter() failed %s", err)
	}

	dispatchTable := map[string]messageHandler{
		"archive-key-entire": handleArchiveKeyEntire,
		"archive-key-start":  handleArchiveKeyStart,
		"archive-key-next":   handleArchiveKeyNext,
		"archive-key-final":  handleArchiveKeyFinal,
		"archive-key-cancel": handleArchiveKeyCancel,
		"destroy-key":        handleDestroyKey}
	/*
		"start-conjoined-archive":  handleStartConjoinedArchive,
		"abort-conjoined-archive":  handleAbortConjoinedArchive,
		"finish-conjoined-archive": handleFinishConjoinedArchive}
	*/
	pushSockets := make(map[string]*zmq4.Socket)

	go func() {
		for message := range messageChan {
			handler, ok := dispatchTable[message.Type]
			if !ok {
				fog.Error("Unknown message type %s %s",
					message.Type, message.Marshalled)
				continue
			}
			reply, err := handler(message)
			if err != nil {
				fog.Error("error handling '%s' %s", message.Type, err)
				continue
			}

			marshalledReply, err := json.Marshal(reply.MessageMap)
			if err != nil {
				fog.Error("unable to marshall reply %s %s", reply, err)
				continue
			}

			var pushSocket *zmq4.Socket
			pushSocket, ok = pushSockets[reply.ClientAddress]
			if !ok {
				fog.Info("creating PUSH socket to %s", reply.ClientAddress)
				if pushSocket, err = createPushSocket(); err != nil {
					fog.Error("Unable to create PUSH socket for %s %s",
						reply.ClientAddress, err)
					continue
				}
				if err = pushSocket.Connect(reply.ClientAddress); err != nil {
					fog.Error("Unable to Connect PUSH socket to %s %s",
						reply.ClientAddress, err)
					pushSocket.Close()
					continue
				}
				pushSockets[reply.ClientAddress] = pushSocket
			}

			if _, err = pushSocket.SendMessage(marshalledReply); err != nil {
				fog.Error("pushSocket SendMessage to %s failed %s",
					reply.ClientAddress, err)
				pushSocket.Close()
				delete(pushSockets, reply.ClientAddress)
				continue
			}
		}

	}()

	return messageChan
}

func createPushSocket() (*zmq4.Socket, error) {
	var err error
	var pushSocket *zmq4.Socket

	if pushSocket, err = zmq4.NewSocket(zmq4.PUSH); err != nil {
		return nil, fmt.Errorf("NewSocket %s", err)
	}

	if err = pushSocket.SetSndhwm(pushSocketSendHWM); err != nil {
		return nil, fmt.Errorf("pushSocket.SetSndhwm(%d) %s",
			pushSocketSendHWM, err)
	}

	return pushSocket, nil
}

func handleArchiveKeyEntire(message types.Message) (Reply, error) {
	var archiveKeyEntire msg.ArchiveKeyEntire
	var md5Digest []byte
	var err error

	archiveKeyEntire, err = msg.UnmarshalArchiveKeyEntire(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalArchiveKeyEntire failed %s", err)
	}

	reply := createReply("archive-key-final", message.ID,
		archiveKeyEntire.UserRequestID, archiveKeyEntire.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyEntire.UserRequestID, archiveKeyEntire.UnifiedID,
		archiveKeyEntire.ConjoinedPart, archiveKeyEntire.SegmentNum, archiveKeyEntire.Key)
	lgr.Info("archive-key-entire")

	if archiveKeyEntire.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", archiveKeyEntire.SegmentSize,
			len(message.Data))
		reply.MessageMap["result"] = "size-mismatch"
		reply.MessageMap["error-message"] = "segment size does not match expected value"
		return reply, nil
	}

	md5Digest, err = base64.StdEncoding.DecodeString(archiveKeyEntire.EncodedSegmentMD5Digest)
	if err != nil {
		return Reply{}, err
	}
	if !MD5DigestMatches(message.Data, md5Digest) {
		lgr.Error("md5 mismatch")
		reply.MessageMap["result"] = "md5-mismatch"
		reply.MessageMap["error-message"] = "segment md5 does not match expected value"
		return reply, nil
	}

	err = nimbusioWriter.StartSegment(lgr, archiveKeyEntire.Segment,
		archiveKeyEntire.NodeNames)
	if err != nil {
		lgr.Error("StartSegment: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	err = nimbusioWriter.StoreSequence(lgr, archiveKeyEntire.Segment,
		archiveKeyEntire.Sequence, message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	metaData := msg.GetMetaFromJSON(message.Marshalled)

	err = nimbusioWriter.FinishSegment(lgr, archiveKeyEntire.Segment,
		archiveKeyEntire.File, metaData)
	if err != nil {
		lgr.Error("FinishSegment: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func handleArchiveKeyStart(message types.Message) (Reply, error) {
	var archiveKeyStart msg.ArchiveKeyStart
	var md5Digest []byte
	var err error

	archiveKeyStart, err = msg.UnmarshalArchiveKeyStart(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalArchiveKeyStart failed %s", err)
	}

	reply := createReply("archive-key-final", message.ID,
		archiveKeyStart.UserRequestID, archiveKeyStart.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyStart.UserRequestID, archiveKeyStart.UnifiedID,
		archiveKeyStart.ConjoinedPart, archiveKeyStart.SegmentNum, archiveKeyStart.Key)
	lgr.Info("archive-key-Start")

	if archiveKeyStart.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", archiveKeyStart.SegmentSize,
			len(message.Data))
		reply.MessageMap["result"] = "size-mismatch"
		reply.MessageMap["error-message"] = "segment size does not match expected value"
		return reply, nil
	}

	md5Digest, err = base64.StdEncoding.DecodeString(archiveKeyStart.EncodedSegmentMD5Digest)
	if err != nil {
		return Reply{}, err
	}
	if !MD5DigestMatches(message.Data, md5Digest) {
		lgr.Error("md5 mismatch")
		reply.MessageMap["result"] = "md5-mismatch"
		reply.MessageMap["error-message"] = "segment md5 does not match expected value"
		return reply, nil
	}

	err = nimbusioWriter.StartSegment(lgr, archiveKeyStart.Segment,
		archiveKeyStart.NodeNames)
	if err != nil {
		lgr.Error("StartSegment: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	err = nimbusioWriter.StoreSequence(lgr, archiveKeyStart.Segment,
		archiveKeyStart.Sequence, message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func handleArchiveKeyNext(message types.Message) (Reply, error) {
	var archiveKeyNext msg.ArchiveKeyNext
	var md5Digest []byte
	var err error

	archiveKeyNext, err = msg.UnmarshalArchiveKeyNext(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalArchiveKeyNext failed %s", err)
	}

	reply := createReply("archive-key-final", message.ID,
		archiveKeyNext.UserRequestID, archiveKeyNext.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyNext.UserRequestID, archiveKeyNext.UnifiedID,
		archiveKeyNext.ConjoinedPart, archiveKeyNext.SegmentNum, archiveKeyNext.Key)
	lgr.Info("archive-key-Next")

	if archiveKeyNext.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", archiveKeyNext.SegmentSize,
			len(message.Data))
		reply.MessageMap["result"] = "size-mismatch"
		reply.MessageMap["error-message"] = "segment size does not match expected value"
		return reply, nil
	}

	md5Digest, err = base64.StdEncoding.DecodeString(archiveKeyNext.EncodedSegmentMD5Digest)
	if err != nil {
		return Reply{}, err
	}
	if !MD5DigestMatches(message.Data, md5Digest) {
		lgr.Error("md5 mismatch")
		reply.MessageMap["result"] = "md5-mismatch"
		reply.MessageMap["error-message"] = "segment md5 does not match expected value"
		return reply, nil
	}

	err = nimbusioWriter.StoreSequence(lgr, archiveKeyNext.Segment,
		archiveKeyNext.Sequence, message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func handleArchiveKeyFinal(message types.Message) (Reply, error) {
	var archiveKeyFinal msg.ArchiveKeyFinal
	var md5Digest []byte
	var err error

	archiveKeyFinal, err = msg.UnmarshalArchiveKeyFinal(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalArchiveKeyFinal failed %s", err)
	}

	reply := createReply("archive-key-final", message.ID,
		archiveKeyFinal.UserRequestID, archiveKeyFinal.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyFinal.UserRequestID, archiveKeyFinal.UnifiedID,
		archiveKeyFinal.ConjoinedPart, archiveKeyFinal.SegmentNum, archiveKeyFinal.Key)
	lgr.Info("archive-key-Final")

	if archiveKeyFinal.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", archiveKeyFinal.SegmentSize,
			len(message.Data))
		reply.MessageMap["result"] = "size-mismatch"
		reply.MessageMap["error-message"] = "segment size does not match expected value"
		return reply, nil
	}

	md5Digest, err = base64.StdEncoding.DecodeString(archiveKeyFinal.EncodedSegmentMD5Digest)
	if err != nil {
		return Reply{}, err
	}
	if !MD5DigestMatches(message.Data, md5Digest) {
		lgr.Error("md5 mismatch")
		reply.MessageMap["result"] = "md5-mismatch"
		reply.MessageMap["error-message"] = "segment md5 does not match expected value"
		return reply, nil
	}

	err = nimbusioWriter.StoreSequence(lgr, archiveKeyFinal.Segment,
		archiveKeyFinal.Sequence, message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	metaData := msg.GetMetaFromJSON(message.Marshalled)

	err = nimbusioWriter.FinishSegment(lgr, archiveKeyFinal.Segment,
		archiveKeyFinal.File, metaData)
	if err != nil {
		lgr.Error("FinishSegment: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func handleArchiveKeyCancel(message types.Message) (Reply, error) {
	var archiveKeyCancel msg.ArchiveKeyCancel
	var err error

	archiveKeyCancel, err = msg.UnmarshalArchiveKeyCancel(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalArchiveKeyCancel failed %s", err)
	}

	reply := createReply("archive-key-cancel", message.ID,
		archiveKeyCancel.UserRequestID, archiveKeyCancel.ReturnAddress)

	lgr := logger.NewLogger(archiveKeyCancel.UserRequestID, archiveKeyCancel.UnifiedID,
		archiveKeyCancel.ConjoinedPart, archiveKeyCancel.SegmentNum, "")
	lgr.Info("archive-key-cancel")

	if err = nimbusioWriter.CancelSegment(lgr, archiveKeyCancel); err != nil {
		lgr.Error("CancelSegment: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func handleDestroyKey(message types.Message) (Reply, error) {
	var destroyKey msg.DestroyKey
	var err error

	destroyKey, err = msg.UnmarshalDestroyKey(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalDestroyKey failed %s", err)
	}

	reply := createReply("destroy-key", message.ID,
		destroyKey.UserRequestID, destroyKey.ReturnAddress)

	lgr := logger.NewLogger(destroyKey.UserRequestID, destroyKey.UnifiedID,
		destroyKey.ConjoinedPart, destroyKey.SegmentNum, destroyKey.Key)
	lgr.Info("archive-key-cancel")

	if err = nimbusioWriter.DestroyKey(lgr, destroyKey); err != nil {
		lgr.Error("DestroyKey: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func handleStartConjoinedArchive(message types.Message) (Reply, error) {
	var conjoined msg.Conjoined
	var err error

	conjoined, err = msg.UnmarshalConjoined(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalConjoined failed %s", err)
	}

	reply := createReply("start-conjoined-archive", message.ID,
		conjoined.UserRequestID, conjoined.ReturnAddress)

	lgr := logger.NewLogger(conjoined.UserRequestID, conjoined.UnifiedID,
		0, 0, conjoined.Key)
	lgr.Info("start-conjoined-archive (%d)", conjoined.CollectionID)

	if err = nimbusioWriter.StartConjoinedArchive(lgr, conjoined); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func handleAbortConjoinedArchive(message types.Message) (Reply, error) {
	var conjoined msg.Conjoined
	var err error

	conjoined, err = msg.UnmarshalConjoined(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalConjoined failed %s", err)
	}

	reply := createReply("abort-conjoined-archive", message.ID,
		conjoined.UserRequestID, conjoined.ReturnAddress)

	lgr := logger.NewLogger(conjoined.UserRequestID, conjoined.UnifiedID,
		0, 0, conjoined.Key)
	lgr.Info("abort-conjoined-archive (%d)", conjoined.CollectionID)

	if err = nimbusioWriter.AbortConjoinedArchive(lgr, conjoined); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func handleFinishConjoinedArchive(message types.Message) (Reply, error) {
	var conjoined msg.Conjoined
	var err error

	conjoined, err = msg.UnmarshalConjoined(message.Marshalled)
	if err != nil {
		return Reply{}, fmt.Errorf("UnmarshalConjoined failed %s", err)
	}

	reply := createReply("finish-conjoined-archive", message.ID,
		conjoined.UserRequestID, conjoined.ReturnAddress)

	lgr := logger.NewLogger(conjoined.UserRequestID, conjoined.UnifiedID,
		0, 0, conjoined.Key)
	lgr.Info("finish-conjoined-archive (%d)", conjoined.CollectionID)

	if err = nimbusioWriter.FinishConjoinedArchive(lgr, conjoined); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		reply.MessageMap["result"] = "error"
		reply.MessageMap["error-message"] = err.Error()
		return reply, nil
	}

	reply.MessageMap["result"] = "success"
	reply.MessageMap["error-message"] = ""

	return reply, nil
}

func createReply(messageType, messageID, userRequestID string,
	returnAddress msg.ReturnAddress) Reply {
	var reply Reply

	reply.ClientAddress = returnAddress.ClientAddress
	reply.MessageMap = make(map[string]interface{})

	reply.MessageMap["message-type"] = messageType + "-reply"
	reply.MessageMap["client-tag"] = returnAddress.ClientTag
	reply.MessageMap["client-address"] = returnAddress.ClientAddress
	reply.MessageMap["user-request-id"] = userRequestID
	reply.MessageMap["message-id"] = messageID
	reply.MessageMap["result"] = nil
	reply.MessageMap["error-message"] = nil

	return reply
}

func MD5DigestMatches(data []byte, md5Digest []byte) bool {
	hasher := md5.New()
	hasher.Write(data)
	dataMd5Digest := hasher.Sum(nil)

	return bytes.Equal(dataMd5Digest, md5Digest)
}
