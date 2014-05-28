package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pebbe/zmq4"

	"fog"
)

type MessageMap map[string]interface{}
type Message struct {
	Type          string
	ID            string
	ClientTag     string
	ClientAddress string
	UserRequestID string
	Map           MessageMap
	Data          []byte
}

type ackOnlyMessageHandler func(Message)

// NewWriterSocketHandler returns a function suitable for use as a handler
// by zmq.Reactor
func NewWriterSocketHandler(writerSocket *zmq4.Socket,
	messageChan chan<- Message) func(zmq4.State) error {

	// these messages get only and ack, not a reply
	var ackOnlyMessages = map[string]ackOnlyMessageHandler{
		"ping": handlePing,
		"resilient-server-handshake": handleHandshake,
		"resilient-server-signoff":   handleSignoff}

	return func(_ zmq4.State) error {
		var err error
		var ok bool

		marshalledMessage, err := writerSocket.RecvMessage(0)
		if err != nil {
			return fmt.Errorf("RecvMessage %s", err)
		}

		var message Message
		err = json.Unmarshal([]byte(marshalledMessage[0]), &message.Map)
		if err != nil {
			return fmt.Errorf("Unmarshal %s", err)
		}

		// we need to cast the common items used in a reply
		if err = castCommonItems(&message); err != nil {
			return err
		}

		message.Data = []byte(strings.Join(marshalledMessage[1:], ""))

		reply := MessageMap{
			"message-type":  "resilient-server-ack",
			"message-id":    message.ID,
			"incoming-type": message.Type,
			"accepted":      true}

		marshalledReply, err := json.Marshal(reply)
		if err != nil {
			return fmt.Errorf("Marshal %s", err)
		}

		_, err = writerSocket.SendMessage([]string{string(marshalledReply)})
		if err != nil {
			return fmt.Errorf("SendMessage %s", err)
		}

		/* ---------------------------------------------------------------
		** 2014-05-21 dougfort:
		** The python version of ResiliantServer maintains a dict of
		** "client-address" for sending replies, but IMO we don't need
		** that here because every message contains "client-address"
		** so we can decouple the reply.
		** -------------------------------------------------------------*/
		handler, ok := ackOnlyMessages[message.Type]
		if ok {
			handler(message)
			return nil
		}

		fog.Debug("writer-socket-handler received %s %s",
			message.Type, message.ID)
		messageChan <- message

		return nil
	}
}

func castCommonItems(message *Message) error {
	var ok bool
	var userRequestID interface{}

	if message.Type, ok = message.Map["message-type"].(string); !ok {
		return fmt.Errorf("unparseable message-type %T, %s",
			message.Map["message-type"], message.Map["message-type"])
	}

	if message.ID, ok = message.Map["message-id"].(string); !ok {
		return fmt.Errorf("unparseable message-id %T, %s",
			message.Map["message-id"], message.Map["message-id"])
	}

	if message.ClientTag, ok = message.Map["client-tag"].(string); !ok {
		return fmt.Errorf("unparseable client-tag %T, %s",
			message.Map["client-tag"], message.Map["client-tag"])
	}

	if message.ClientAddress, ok = message.Map["client-address"].(string); !ok {
		return fmt.Errorf("unparseable client-address %T, %s",
			message.Map["client-address"], message.Map["client-address"])
	}

	// the handshake message doesn't have user-request-id
	userRequestID, ok = message.Map["user-request-id"]
	if ok {
		if message.UserRequestID, ok = userRequestID.(string); !ok {
			return fmt.Errorf("unparseable user-request-id %T, %s",
				message.Map["user-request-id"], message.Map)
		}
	}

	return nil

}

func handlePing(_ Message) {

}

func handleHandshake(message Message) {
	fog.Info("handshake from %s", message.ClientAddress)
}

func handleSignoff(message Message) {
	fog.Info("signoff from   %s", message.ClientAddress)
}
