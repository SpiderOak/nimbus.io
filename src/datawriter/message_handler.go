package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pebbe/zmq4"

	"fog"
	"tools"

	"datawriter/logger"
	"datawriter/nodedb"
	"datawriter/types"
	"datawriter/writer"
)

const (
	messageChanCapacity = 100
	pushSocketSendHWM   = 10
)

// message handler takes a message and returns a reply
type messageHandler func(message types.Message) types.MessageMap

var (
	nodeIDMap      map[string]uint32
	nimbusioWriter writer.NimbusioWriter
)

func NewMessageHandler() chan<- types.Message {
	var err error

	messageChan := make(chan types.Message, messageChanCapacity)

	if nodeIDMap, err = tools.GetNodeIDMap(); err != nil {
		fog.Critical("NewMessageHandler: tools.GetNodeIDMap() failed %s", err)
	}
	fog.Debug("nodeIDMap = %s", nodeIDMap)

	if err = nodedb.Initialize(); err != nil {
		fog.Critical("NewMessageHandler: nodedb.Initialize failed %s", err)
	}

	if nimbusioWriter, err = writer.NewNimbusioWriter(); err != nil {
		fog.Critical("NewMessageHandler: NewNimbusioWriter() failed %s", err)
	}

	dispatchTable := map[string]messageHandler{
		"archive-key-entire":       handleArchiveKeyEntire,
		"archive-key-start":        handleArchiveKeyStart,
		"archive-key-next":         handleArchiveKeyNext,
		"archive-key-final":        handleArchiveKeyFinal,
		"archive-key-cancel":       handleArchiveKeyCancel,
		"destroy-key":              handleDestroyKey,
		"start-conjoined-archive":  handleStartConjoinedArchive,
		"abort-conjoined-archive":  handleAbortConjoinedArchive,
		"finish-conjoined-archive": handleFinishConjoinedArchive,
		"web-writer-start":         handleWebWriterStart}
	pushSockets := make(map[string]*zmq4.Socket)

	go func() {
		for message := range messageChan {
			handler, ok := dispatchTable[message.Type]
			if !ok {
				fog.Error("Unknown message type %s from %s",
					message.Type, message.ClientAddress)
				continue
			}
			reply := handler(message)

			marshalledReply, err := json.Marshal(reply)
			if err != nil {
				fog.Error("unable to marshall reply %s %s", reply, err)
				continue
			}

			var pushSocket *zmq4.Socket
			pushSocket, ok = pushSockets[message.ClientAddress]
			if !ok {
				fog.Info("creating PUSH socket to %s", message.ClientAddress)
				if pushSocket, err = createPushSocket(); err != nil {
					fog.Error("Unable to create PUSH socket for %s %s",
						message.ClientAddress, err)
					continue
				}
				if err = pushSocket.Connect(message.ClientAddress); err != nil {
					fog.Error("Unable to Connect PUSH socket to %s %s",
						message.ClientAddress, err)
					pushSocket.Close()
					continue
				}
				pushSockets[message.ClientAddress] = pushSocket
			}

			if _, err = pushSocket.SendMessage(marshalledReply); err != nil {
				fog.Error("pushSocket SendMessage to %s failed %s",
					message.ClientAddress, err)
				pushSocket.Close()
				delete(pushSockets, message.ClientAddress)
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

func handleArchiveKeyEntire(message types.Message) types.MessageMap {
	var segmentEntry types.SegmentEntry
	var sequenceEntry types.SequenceEntry
	var fileEntry types.FileEntry
	var err error

	reply := createReply(message)

	if segmentEntry, err = parseSegmentEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if sequenceEntry, err = parseSequenceEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if fileEntry, err = parseFileEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, segmentEntry.UnifiedID,
		segmentEntry.ConjoinedPart, segmentEntry.SegmentNum, segmentEntry.Key)
	lgr.Info("archive-key-entire")

	if sequenceEntry.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", sequenceEntry.SegmentSize,
			len(message.Data))
		reply["result"] = "size-mismatch"
		reply["error-message"] = "segment size does not match expected value"
		return reply
	}

	if !MD5DigestMatches(message.Data, sequenceEntry.MD5Digest) {
		lgr.Error("md5 mismatch")
		reply["result"] = "md5-mismatch"
		reply["error-message"] = "segment md5 does not match expected value"
		return reply
	}

	if err = nimbusioWriter.StartSegment(lgr, segmentEntry); err != nil {
		lgr.Error("StartSegment: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	err = nimbusioWriter.StoreSequence(lgr, segmentEntry, sequenceEntry,
		message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if err = nimbusioWriter.FinishSegment(lgr, segmentEntry, fileEntry); err != nil {
		lgr.Error("FinishSegment: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleArchiveKeyStart(message types.Message) types.MessageMap {
	var segmentEntry types.SegmentEntry
	var sequenceEntry types.SequenceEntry
	var err error

	reply := createReply(message)

	if segmentEntry, err = parseSegmentEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if sequenceEntry, err = parseSequenceEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, segmentEntry.UnifiedID,
		segmentEntry.ConjoinedPart, segmentEntry.SegmentNum, segmentEntry.Key)
	lgr.Info("archive-key-start")

	if sequenceEntry.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", sequenceEntry.SegmentSize,
			len(message.Data))
		reply["result"] = "size-mismatch"
		reply["error-message"] = "segment size does not match expected value"
		return reply
	}

	if !MD5DigestMatches(message.Data, sequenceEntry.MD5Digest) {
		lgr.Error("md5 mismatch")
		reply["result"] = "md5-mismatch"
		reply["error-message"] = "segment md5 does not match expected value"
		return reply
	}

	if err = nimbusioWriter.StartSegment(lgr, segmentEntry); err != nil {
		lgr.Error("StartSegment: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	err = nimbusioWriter.StoreSequence(lgr, segmentEntry, sequenceEntry,
		message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleArchiveKeyNext(message types.Message) types.MessageMap {
	var segmentEntry types.SegmentEntry
	var sequenceEntry types.SequenceEntry
	var err error

	reply := createReply(message)

	if segmentEntry, err = parseSegmentEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if sequenceEntry, err = parseSequenceEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, segmentEntry.UnifiedID,
		segmentEntry.ConjoinedPart, segmentEntry.SegmentNum, segmentEntry.Key)
	lgr.Info("archive-key-next")

	if sequenceEntry.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", sequenceEntry.SegmentSize,
			len(message.Data))
		reply["result"] = "size-mismatch"
		reply["error-message"] = "segment size does not match expected value"
		return reply
	}

	if !MD5DigestMatches(message.Data, sequenceEntry.MD5Digest) {
		lgr.Error("md5 mismatch")
		reply["result"] = "md5-mismatch"
		reply["error-message"] = "segment md5 does not match expected value"
		return reply
	}

	err = nimbusioWriter.StoreSequence(lgr, segmentEntry, sequenceEntry,
		message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleArchiveKeyFinal(message types.Message) types.MessageMap {
	var segmentEntry types.SegmentEntry
	var sequenceEntry types.SequenceEntry
	var fileEntry types.FileEntry
	var err error

	reply := createReply(message)

	if segmentEntry, err = parseSegmentEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if sequenceEntry, err = parseSequenceEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if fileEntry, err = parseFileEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, segmentEntry.UnifiedID,
		segmentEntry.ConjoinedPart, segmentEntry.SegmentNum, segmentEntry.Key)
	lgr.Info("archive-key-final")

	if sequenceEntry.SegmentSize != uint64(len(message.Data)) {
		lgr.Error("size mismatch (%d != %d)", sequenceEntry.SegmentSize,
			len(message.Data))
		reply["result"] = "size-mismatch"
		reply["error-message"] = "segment size does not match expected value"
		return reply
	}

	if !MD5DigestMatches(message.Data, sequenceEntry.MD5Digest) {
		lgr.Error("md5 mismatch")
		reply["result"] = "md5-mismatch"
		reply["error-message"] = "segment md5 does not match expected value"
		return reply
	}

	err = nimbusioWriter.StoreSequence(lgr, segmentEntry, sequenceEntry,
		message.Data)
	if err != nil {
		lgr.Error("StoreSequence: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if err = nimbusioWriter.FinishSegment(lgr, segmentEntry, fileEntry); err != nil {
		lgr.Error("FinishSegment: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleArchiveKeyCancel(message types.Message) types.MessageMap {
	var cancelEntry types.CancelEntry
	var err error

	reply := createReply(message)

	if cancelEntry, err = parseCancelEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, cancelEntry.UnifiedID,
		cancelEntry.ConjoinedPart, cancelEntry.SegmentNum, "")
	lgr.Info("archive-key-cancel")

	if err = nimbusioWriter.CancelSegment(lgr, cancelEntry); err != nil {
		lgr.Error("CancelSegment: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleDestroyKey(message types.Message) types.MessageMap {
	var segmentEntry types.SegmentEntry
	var rawUnifiedIDToDestroy float64
	var unifiedIDToDestroy uint64
	var err error
	var ok bool

	reply := createReply(message)

	if segmentEntry, err = parseSegmentEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	if message.Map["unified-id-to-Destroy"] != nil {
		if rawUnifiedIDToDestroy, ok = message.Map["unified-id-to-Destroy"].(float64); !ok {
			reply["result"] = "error"
			reply["error-message"] = err.Error()
			return reply
		}
		unifiedIDToDestroy = uint64(rawUnifiedIDToDestroy)
	}

	lgr := logger.NewLogger(message.UserRequestID, segmentEntry.UnifiedID,
		segmentEntry.ConjoinedPart, segmentEntry.SegmentNum, segmentEntry.Key)
	lgr.Info("destroy-key (%d)", unifiedIDToDestroy)

	if err = nimbusioWriter.DestroyKey(lgr, segmentEntry, unifiedIDToDestroy); err != nil {
		lgr.Error("DestroyKey: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleStartConjoinedArchive(message types.Message) types.MessageMap {
	var conjoinedEntry types.ConjoinedEntry
	var err error

	reply := createReply(message)

	if conjoinedEntry, err = parseConjoinedEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, conjoinedEntry.UnifiedID,
		0, 0, conjoinedEntry.Key)
	lgr.Info("start-conjoined-archive (%d)", conjoinedEntry.CollectionID)

	if err = nimbusioWriter.StartConjoinedArchive(lgr, conjoinedEntry); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleAbortConjoinedArchive(message types.Message) types.MessageMap {
	var conjoinedEntry types.ConjoinedEntry
	var err error

	reply := createReply(message)

	if conjoinedEntry, err = parseConjoinedEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, conjoinedEntry.UnifiedID,
		0, 0, conjoinedEntry.Key)
	lgr.Info("abort-conjoined-archive (%d)", conjoinedEntry.CollectionID)

	if err = nimbusioWriter.AbortConjoinedArchive(lgr, conjoinedEntry); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleFinishConjoinedArchive(message types.Message) types.MessageMap {
	var conjoinedEntry types.ConjoinedEntry
	var err error

	reply := createReply(message)

	if conjoinedEntry, err = parseConjoinedEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, conjoinedEntry.UnifiedID,
		0, 0, conjoinedEntry.Key)
	lgr.Info("finish-conjoined-archive (%d)", conjoinedEntry.CollectionID)

	if err = nimbusioWriter.FinishConjoinedArchive(lgr, conjoinedEntry); err != nil {
		lgr.Error("StartConjoinedArchive: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func handleWebWriterStart(message types.Message) types.MessageMap {
	var webWriterStartEntry types.WebWriterStartEntry
	var err error

	reply := createReply(message)

	if webWriterStartEntry, err = parseWebWriterStartEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	lgr := logger.NewLogger(message.UserRequestID, webWriterStartEntry.UnifiedID,
		0, 0, "")
	lgr.Info("web-writer-start %d", webWriterStartEntry.SourceNodeID)

	if err = nimbusioWriter.CancelSegmentsFromNode(lgr, webWriterStartEntry); err != nil {
		lgr.Error("CancelSegmentsFromNode: %s", err)
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	reply["result"] = "success"
	reply["error-message"] = ""

	return reply
}

func createReply(message types.Message) types.MessageMap {
	reply := make(types.MessageMap)
	if message.Type == "archive-key-entire" {
		reply["message-type"] = "archive-key-final-reply"
	} else {
		reply["message-type"] = message.Type + "-reply"
	}

	reply["client-tag"] = message.ClientTag
	reply["client-address"] = message.ClientAddress
	reply["user-request-id"] = message.UserRequestID
	reply["message-id"] = message.ID
	reply["result"] = nil
	reply["error-message"] = nil

	return reply
}

func parseSegmentEntry(message types.Message) (types.SegmentEntry, error) {
	var entry types.SegmentEntry
	var ok bool
	var err error
	var collectionID float64
	var unifiedID float64
	var conjoinedPart float64
	var segmentNum float64
	var timestampRepr string
	var sourceNodeName string
	var handoffNodeName string

	if collectionID, ok = message.Map["collection-id"].(float64); !ok {
		return entry, fmt.Errorf("unparseable collection-id %T, %s",
			message.Map["collection-id"], message.Map["collection-id"])
	}
	entry.CollectionID = uint32(collectionID)

	if entry.Key, ok = message.Map["key"].(string); !ok {
		return entry, fmt.Errorf("unparseable key %T, %s",
			message.Map["key"], message.Map["key"])
	}

	if unifiedID, ok = message.Map["unified-id"].(float64); !ok {
		return entry, fmt.Errorf("unparseable unified-id %T, %s",
			message.Map["unified-id"], message.Map["unified-id"])
	}
	entry.UnifiedID = uint64(unifiedID)

	if timestampRepr, ok = message.Map["timestamp-repr"].(string); !ok {
		return entry, fmt.Errorf("unparseable timestamp-repr %T, %s",
			message.Map["timestamp-repr"], message.Map["timestamp-repr"])
	}
	if entry.Timestamp, err = ParseTimestampRepr(timestampRepr); err != nil {
		return entry, fmt.Errorf("unable to parse %s %s", timestampRepr, err)
	}

	if message.Map["conjoined-part"] != nil {
		if conjoinedPart, ok = message.Map["conjoined-part"].(float64); !ok {
			return entry, fmt.Errorf("unparseable conjoined-part %T, %s",
				message.Map["conjoined-part"], message.Map["conjoined-part"])
		}
		entry.ConjoinedPart = uint32(conjoinedPart)
	}

	if segmentNum, ok = message.Map["segment-num"].(float64); !ok {
		return entry, fmt.Errorf("unparseable segment-num %T, %s",
			message.Map["segment-num"], message.Map["segment-num"])
	}
	entry.SegmentNum = uint8(segmentNum)

	sourceNodeName, ok = message.Map["source-node-name"].(string)
	if !ok {
		return entry, fmt.Errorf("unparseable source-node-name %T, %s",
			message.Map["source-node-name"], message.Map["source-node-name"])
	}
	entry.SourceNodeID, ok = nodeIDMap[sourceNodeName]
	if !ok {
		return entry, fmt.Errorf("unknown source-node-name %s",
			message.Map["source-node-name"])
	}

	if message.Map["handoff-node-name"] != nil {
		handoffNodeName, ok = message.Map["handoff-node-name"].(string)
		if !ok {
			return entry, fmt.Errorf("unparseable handoff-node-name %T, %s",
				message.Map["handoff-node-name"], message.Map["handoff-node-name"])
		}
		entry.HandoffNodeID, ok = nodeIDMap[handoffNodeName]
		if !ok {
			return entry, fmt.Errorf("unknown handoff-node-name %s",
				message.Map["handoff-node-name"])
		}
	}

	return entry, nil
}

func parseSequenceEntry(message types.Message) (types.SequenceEntry, error) {
	var entry types.SequenceEntry
	var err error
	var ok bool
	var rawSequenceNum interface{}
	var sequenceNum float64
	var segmentSize float64
	var zfecPaddingSize float64
	var encodedMD5Digest string
	var adler32 float64

	// if we don't have a sequence num, use 0 (archive-key-entire)
	rawSequenceNum, ok = message.Map["sequence-num"]
	if ok {
		if sequenceNum, ok = rawSequenceNum.(float64); !ok {
			return entry, fmt.Errorf("unparseable sequence-num %T, %s",
				message.Map["sequence-num"], message.Map["sequence-num"])
		}
		entry.SequenceNum = uint32(sequenceNum)
	}

	if segmentSize, ok = message.Map["segment-size"].(float64); !ok {
		return entry, fmt.Errorf("unparseable segment-size %T, %s",
			message.Map["segment-size"], message.Map["segment-size"])
	}
	entry.SegmentSize = uint64(segmentSize)

	if zfecPaddingSize, ok = message.Map["zfec-padding-size"].(float64); !ok {
		return entry, fmt.Errorf("unparseable zfec-padding-size %T, %s",
			message.Map["zfec-padding-size"], message.Map["zfec-padding-size"])
	}
	entry.ZfecPaddingSize = uint32(zfecPaddingSize)

	if encodedMD5Digest, ok = message.Map["segment-md5-digest"].(string); !ok {
		return entry, fmt.Errorf("unparseable segment-md5-digest %T, %s",
			message.Map["segment-md5-digest"], message.Map["segment-md5-digest"])
	}
	entry.MD5Digest, err = base64.StdEncoding.DecodeString(encodedMD5Digest)
	if err != nil {
		return entry, fmt.Errorf("can't decode segment-md5-digest %s",
			encodedMD5Digest)
	}

	if adler32, ok = message.Map["segment-adler32"].(float64); !ok {
		return entry, fmt.Errorf("unparseable segment-adler32 %T, %s",
			message.Map["segment-adler32"], message.Map["segment-adler32"])
	}
	entry.Adler32 = int32(adler32)

	return entry, nil
}

func parseFileEntry(message types.Message) (types.FileEntry, error) {
	var entry types.FileEntry
	var err error
	var ok bool
	var fileSize float64
	var encodedMD5Digest string
	var adler32 float64

	if fileSize, ok = message.Map["file-size"].(float64); !ok {
		return entry, fmt.Errorf("unparseable file-size %T, %s",
			message.Map["file-size"], message.Map["file-size"])
	}
	entry.FileSize = uint64(fileSize)

	if encodedMD5Digest, ok = message.Map["file-hash"].(string); !ok {
		return entry, fmt.Errorf("unparseable file-hash %T, %s",
			message.Map["file-hash"], message.Map["file-hash"])
	}
	entry.MD5Digest, err = base64.StdEncoding.DecodeString(encodedMD5Digest)
	if err != nil {
		return entry, fmt.Errorf("can't decode segment-md5-digest %s",
			encodedMD5Digest)
	}

	if adler32, ok = message.Map["file-adler32"].(float64); !ok {
		return entry, fmt.Errorf("unparseable file-adler32 %T, %s",
			message.Map["file-adler32"], message.Map["file-adler32"])
	}
	entry.Adler32 = int32(adler32)

	for key := range message.Map {
		if strings.HasPrefix(key, "__nimbus_io__") {
			var metaEntry types.MetaEntry
			metaEntry.Key = key[len("__nimbus_io__"):]
			if metaEntry.Value, ok = message.Map[key].(string); !ok {
				return entry, fmt.Errorf("unparseable %s %T, %s",
					key, message.Map[key], message.Map[key])
			}
			entry.MetaData = append(entry.MetaData, metaEntry)
		}
	}

	return entry, nil
}

func parseCancelEntry(message types.Message) (types.CancelEntry, error) {
	var entry types.CancelEntry
	var ok bool
	var unifiedID float64
	var conjoinedPart float64
	var segmentNum float64

	if unifiedID, ok = message.Map["unified-id"].(float64); !ok {
		return entry, fmt.Errorf("unparseable unified-id %T, %s",
			message.Map["unified-id"], message.Map["unified-id"])
	}
	entry.UnifiedID = uint64(unifiedID)

	if message.Map["conjoined-part"] != nil {
		if conjoinedPart, ok = message.Map["conjoined-part"].(float64); !ok {
			return entry, fmt.Errorf("unparseable conjoined-part %T, %s",
				message.Map["conjoined-part"], message.Map["conjoined-part"])
		}
		entry.ConjoinedPart = uint32(conjoinedPart)
	}

	if segmentNum, ok = message.Map["segment-num"].(float64); !ok {
		return entry, fmt.Errorf("unparseable segment-num %T, %s",
			message.Map["segment-num"], message.Map["segment-num"])
	}
	entry.SegmentNum = uint8(segmentNum)

	return entry, nil
}

func parseConjoinedEntry(message types.Message) (types.ConjoinedEntry, error) {
	var entry types.ConjoinedEntry
	var ok bool
	var err error
	var collectionID float64
	var unifiedID float64
	var timestampRepr string
	var handoffNodeName string

	if collectionID, ok = message.Map["collection-id"].(float64); !ok {
		return entry, fmt.Errorf("unparseable collection-id %T, %s",
			message.Map["collection-id"], message.Map["collection-id"])
	}
	entry.CollectionID = uint32(collectionID)

	if entry.Key, ok = message.Map["key"].(string); !ok {
		return entry, fmt.Errorf("unparseable key %T, %s",
			message.Map["key"], message.Map["key"])
	}

	if unifiedID, ok = message.Map["unified-id"].(float64); !ok {
		return entry, fmt.Errorf("unparseable unified-id %T, %s",
			message.Map["unified-id"], message.Map["unified-id"])
	}
	entry.UnifiedID = uint64(unifiedID)

	if timestampRepr, ok = message.Map["timestamp-repr"].(string); !ok {
		return entry, fmt.Errorf("unparseable timestamp-repr %T, %s",
			message.Map["timestamp-repr"], message.Map["timestamp-repr"])
	}
	if entry.Timestamp, err = ParseTimestampRepr(timestampRepr); err != nil {
		return entry, fmt.Errorf("unable to parse %s %s", timestampRepr, err)
	}

	if message.Map["handoff-node-name"] != nil {
		handoffNodeName, ok = message.Map["handoff-node-name"].(string)
		if !ok {
			return entry, fmt.Errorf("unparseable handoff-node-name %T, %s",
				message.Map["handoff-node-name"], message.Map["handoff-node-name"])
		}
		entry.HandoffNodeID, ok = nodeIDMap[handoffNodeName]
		if !ok {
			return entry, fmt.Errorf("unknown handoff-node-name %s",
				message.Map["handoff-node-name"])
		}
	}

	return entry, nil
}

func parseWebWriterStartEntry(message types.Message) (types.WebWriterStartEntry, error) {
	var entry types.WebWriterStartEntry
	var ok bool
	var err error
	var unifiedID float64
	var timestampRepr string
	var sourceNodeName string

	if unifiedID, ok = message.Map["unified_id"].(float64); !ok {
		return entry, fmt.Errorf("unparseable unified_id %T, %s",
			message.Map["unified_id"], message.Map["unified_id"])
	}
	entry.UnifiedID = uint64(unifiedID)

	if timestampRepr, ok = message.Map["timestamp_repr"].(string); !ok {
		return entry, fmt.Errorf("unparseable timestamp_repr %T, %s",
			message.Map["timestamp_repr"], message.Map["timestamp_repr"])
	}
	if entry.Timestamp, err = ParseTimestampRepr(timestampRepr); err != nil {
		return entry, fmt.Errorf("unable to parse %s %s", timestampRepr, err)
	}

	sourceNodeName, ok = message.Map["source_node_name"].(string)
	if !ok {
		return entry, fmt.Errorf("unparseable source_node_name %T, %s",
			message.Map["source_node_name"], message.Map["source_node_name"])
	}
	entry.SourceNodeID, ok = nodeIDMap[sourceNodeName]
	if !ok {
		return entry, fmt.Errorf("unknown source_node_name %s",
			message.Map["source_node_name"])
	}

	return entry, nil
}

func MD5DigestMatches(data []byte, md5Digest []byte) bool {
	hasher := md5.New()
	hasher.Write(data)
	dataMd5Digest := hasher.Sum(nil)

	return bytes.Equal(dataMd5Digest, md5Digest)
}
