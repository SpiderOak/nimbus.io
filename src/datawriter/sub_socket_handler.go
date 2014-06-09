package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pebbe/zmq4"

	"fog"

	"datawriter/nodedb"
	"datawriter/types"
)

// NewEventSubSocketHandler returns a function that handles event notifications
func NewEventSubSocketHandler(eventSubSocket *zmq4.Socket) func(zmq4.State) error {

	return func(_ zmq4.State) error {
		var err error
		var ok bool
		var messageMap types.MessageMap
		var messageType string
		var timestampRepr string
		var timestamp time.Time
		var sourceNodeID uint32
		var sourceNodeName string

		marshalledMessage, err := eventSubSocket.RecvMessage(0)
		if err != nil {
			return fmt.Errorf("RecvMessage %s", err)
		}

		// the 0th part should be the topic, we skip that

		err = json.Unmarshal([]byte(marshalledMessage[1]), &messageMap)
		if err != nil {
			return fmt.Errorf("Unmarshal %s", err)
		}

		if messageType, ok = messageMap["message-type"].(string); !ok {
			return fmt.Errorf("unparseable message-type %T, %s",
				messageMap["message-type"], messageMap["message-type"])
		}

		if messageType != "web-writer-start" {
			return fmt.Errorf("unknown message type '%s'", messageType)
		}

		if timestampRepr, ok = messageMap["timestamp_repr"].(string); !ok {
			return fmt.Errorf("unparseable timestamp_repr %T, %s",
				messageMap["timestamp_repr"], messageMap["timestamp_repr"])
		}
		if timestamp, err = ParseTimestampRepr(timestampRepr); err != nil {
			return fmt.Errorf("unable to parse %s %s", timestampRepr, err)
		}

		sourceNodeName, ok = messageMap["source_node_name"].(string)
		if !ok {
			return fmt.Errorf("unparseable source_node_name %T, %s",
				messageMap["source_node_name"], messageMap["source_node_name"])
		}
		sourceNodeID, ok = nodeIDMap[sourceNodeName]
		if !ok {
			return fmt.Errorf("unknown source_node_name %s",
				messageMap["source_node_name"])
		}

		fog.Debug("cancel-segments-from-node %s", sourceNodeName)

		// cancel all all segment rows
		//    * from a specifiic source node
		//    * are in active status
		//    * with a timestamp earlier than the specified time.
		// This is triggered by a web server restart
		stmt := nodedb.Stmts["cancel-segments-from-node"]
		if _, err = stmt.Exec(sourceNodeID, timestamp); err != nil {
			return fmt.Errorf("cancel-segments-from-node %s", err)
		}

		return nil
	}
}
