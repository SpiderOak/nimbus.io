package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pebbe/zmq4"

	"fog"
	"tools"
)

const (
	messageChanCapacity = 100
	pushSocketSendHWM   = 10
)

type SegmentEntry struct {
	CollectionID  uint32
	Key           string
	UnifiedID     uint64
	Timestamp     time.Time
	ConjoinedPart uint32
	SegmentNum    uint8
	SourceNodeID  uint32
	HandoffNodeID uint32
}

func (entry SegmentEntry) String() string {
	return fmt.Sprintf("(%d) %s %d %s %d %d",
		entry.CollectionID,
		entry.Key,
		entry.UnifiedID,
		entry.Timestamp,
		entry.ConjoinedPart,
		entry.SegmentNum)
}

type SequenceEntry struct {
	SequenceNum     uint32
	SegmentSize     uint64
	ZfecPaddingSize uint32
	MD5Digest       []byte
	Adler32         uint32
}

func (entry SequenceEntry) String() string {
	return fmt.Sprintf("%d %d %d %x %d",
		entry.SequenceNum,
		entry.SegmentSize,
		entry.ZfecPaddingSize,
		entry.MD5Digest,
		entry.Adler32)
}

// message handler takes a message and returns a reply
type messageHandler func(message Message) MessageMap

var (
	nodeIDMap map[string]uint32
)

func NewMessageHandler() chan<- Message {
	var err error

	messageChan := make(chan Message, messageChanCapacity)

	if nodeIDMap, err = tools.GetNodeIDMap(); err != nil {
		fog.Critical("NewMessageHandler: tools.GetNodeIDMap() failed %s", err)
	}
	fog.Debug("nodeIDMap = %s", nodeIDMap)

	dispatchTable := map[string]messageHandler{
		"archive-key-entire": handleArchiveKeyEntire}
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

func handleArchiveKeyEntire(message Message) MessageMap {
	var segmentEntry SegmentEntry
	var sequenceEntry SequenceEntry
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

	if sequenceEntry.SegmentSize != uint64(len(message.Data)) {
		fog.Error("%s size mismatch (%d != %d)", segmentEntry,
			sequenceEntry.SegmentSize, len(message.Data))
		reply["result"] = "size-mismatch"
		reply["error-message"] = "segment size does not match expected value"
		return reply
	}

	if !MD5DigestMatches(message.Data, sequenceEntry.MD5Digest) {
		fog.Error("%s md5 mismatch", segmentEntry)
		reply["result"] = "md5-mismatch"
		reply["error-message"] = "segment md5 does not match expected value"
		return reply
	}

	reply["result"] = "error"
	reply["error-message"] = "not implemented"

	return reply
}

func createReply(message Message) MessageMap {
	reply := make(MessageMap)
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

func parseSegmentEntry(message Message) (SegmentEntry, error) {
	var entry SegmentEntry
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

	if conjoinedPart, ok = message.Map["conjoined-part"].(float64); !ok {
		return entry, fmt.Errorf("unparseable conjoined-part %T, %s",
			message.Map["conjoined-part"], message.Map["conjoined-part"])
	}
	entry.ConjoinedPart = uint32(conjoinedPart)

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

func parseSequenceEntry(message Message) (SequenceEntry, error) {
	var entry SequenceEntry
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
	entry.Adler32 = uint32(adler32)

	return entry, nil
}

func MD5DigestMatches(data []byte, md5Digest []byte) bool {
	hasher := md5.New()
	hasher.Write(data)
	dataMd5Digest := hasher.Sum(nil)

	return bytes.Equal(dataMd5Digest, md5Digest)
}
