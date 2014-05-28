package main

import (
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
	var err error

	reply := createReply(message)

	if segmentEntry, err = parseSegmentEntry(message); err != nil {
		reply["result"] = "error"
		reply["error-message"] = err.Error()
		return reply
	}

	fog.Debug("segment: %s", segmentEntry)

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
	var timestampRepr string

	if entry.CollectionID, ok = message.Map["collection-id"].(uint32); !ok {
		return entry, fmt.Errorf("unparseable collection-id %T, %s",
			message.Map["collection-id"], message.Map["collection-id"])
	}

	if entry.Key, ok = message.Map["key"].(string); !ok {
		return entry, fmt.Errorf("unparseable key %T, %s",
			message.Map["key"], message.Map["key"])
	}

	if entry.UnifiedID, ok = message.Map["unified-id"].(uint64); !ok {
		return entry, fmt.Errorf("unparseable unified-id %T, %s",
			message.Map["unified-id"], message.Map["unified-id"])
	}

	if timestampRepr, ok = message.Map["timestamp-repr"].(string); !ok {
		return entry, fmt.Errorf("unparseable timestamp-repr %T, %s",
			message.Map["timestamp-repr"], message.Map["timestamp-repr"])
	}
	if entry.Timestamp, err = ParseTimestampRepr(timestampRepr); err != nil {
		return entry, fmt.Errorf("unable to parse %s %s", timestampRepr, err)
	}

	if entry.ConjoinedPart, ok = message.Map["conjoined-part"].(uint32); !ok {
		return entry, fmt.Errorf("unparseable conjoined-part %T, %s",
			message.Map["conjoined-part"], message.Map["conjoined-part"])
	}

	if entry.SegmentNum, ok = message.Map["segment-num"].(uint8); !ok {
		return entry, fmt.Errorf("unparseable segment-num %T, %s",
			message.Map["segment-num"], message.Map["segment-num"])
	}

	return entry, nil
}
