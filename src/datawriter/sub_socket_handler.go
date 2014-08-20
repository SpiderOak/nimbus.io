package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pebbe/zmq4"

	"centraldb"
	"fog"
	"tools"

	"datawriter/msg"
	"datawriter/nodedb"
)

// NewEventSubSocketHandler returns a function that handles event notifications
func NewEventSubSocketHandler(eventSubSocket *zmq4.Socket) func(zmq4.State) error {
	var nodeIDMap map[string]uint32
	var err error

	if nodeIDMap, err = centraldb.GetNodeIDMap(); err != nil {
		fog.Critical("centraldb.GetNodeIDMap() failed %s", err)
	}

	return func(_ zmq4.State) error {
		var err error
		var ok bool
		var webWriterStart msg.WebWriterStart
		var timestamp time.Time
		var sourceNodeID uint32

		marshalledMessage, err := eventSubSocket.RecvMessage(0)
		if err != nil {
			return fmt.Errorf("RecvMessage %s", err)
		}

		// the 0th part should be the topic, we skip that

		err = json.Unmarshal([]byte(marshalledMessage[1]), &webWriterStart)
		if err != nil {
			return fmt.Errorf("Unmarshal %s", err)
		}

		if webWriterStart.MessageType != "web-writer-start" {
			return fmt.Errorf("unknown message type '%s'",
				webWriterStart.MessageType)
		}

		timestamp, err = tools.ParseTimestampRepr(webWriterStart.TimestampRepr)
		if err != nil {
			return fmt.Errorf("unable to parse %s %s",
				webWriterStart.TimestampRepr, err)
		}

		sourceNodeID, ok = nodeIDMap[webWriterStart.SourceNodeName]
		if !ok {
			return fmt.Errorf("unknown source_node_name %s",
				webWriterStart.SourceNodeName)
		}

		fog.Debug("cancel-segments-from-node %s", webWriterStart.SourceNodeName)

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
