package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pebbe/zmq4"

	"fog"

	"datawriter/msg"
	"datawriter/types"
)

type ackOnlyMessageHandler func(types.Message)

// NewWriterSocketHandler returns a function suitable for use as a handler
// by zmq.Reactor
func NewWriterSocketHandler(writerSocket *zmq4.Socket,
	messageChan chan<- types.Message) func(zmq4.State) error {

	// these messages get only and ack, not a reply
	var ackOnlyMessages = map[string]ackOnlyMessageHandler{
		"ping": handlePing,
		"resilient-server-handshake": handleHandshake,
		"resilient-server-signoff":   handleSignoff}

	return func(_ zmq4.State) error {
		var err error
		var ok bool
		var rawMessage []string
		var message types.Message

		if rawMessage, err = writerSocket.RecvMessage(0); err != nil {
			return fmt.Errorf("RecvMessage %s", err)
		}

		message.Marshalled = rawMessage[0]
		message.Data = []byte(strings.Join(rawMessage[1:], ""))

		if message.Type, err = msg.GetMessageType(message.Marshalled); err != nil {
			return fmt.Errorf("GetMessageType %s", err)
		}

		if message.ID, err = msg.GetMessageID(message.Marshalled); err != nil {
			return fmt.Errorf("GetMessageID %s", err)
		}

		reply := types.MessageMap{
			"message-type":  "resilient-server-ack",
			"message-id":    messageID,
			"incoming-type": messageType,
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

func handlePing(_ types.Message) {

}

func handleHandshake(message types.Message) {
	returnAddress, err := msg.UnmarshalReturnAddress(message.Marshalled)
	if err != nil {
		fog.Error("handleHandshake: unable to unmarshall %s", err)
		return
	}
	fog.Info("handshake from %s", returnAddress.ClientAddress)
}

func handleSignoff(message types.Message) {
	returnAddress, err := msg.UnmarshalReturnAddress(message.Marshalled)
	if err != nil {
		fog.Error("handleSignoff: unable to unmarshall %s", err)
		return
	}
	fog.Info("signoff from %s", message.ClientAddress)
}
