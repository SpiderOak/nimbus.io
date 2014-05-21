package tools

import (
	"encoding/json"

	"github.com/pebbe/zmq4"

	"fog"
)

const (
	messageChanCapacity = 100
	logTag              = "ResilientServer"
)

type Message map[string]string
type MessageWithBody struct {
	Message Message
	Body    [][]byte
}

func NewResilientServer(endpoint string) (<-chan MessageWithBody, error) {
	messageChan := make(chan MessageWithBody, messageChanCapacity)

	var socket *zmq4.Socket
	var err error
	var localMessageMap = map[string]struct{}{
		"ping": struct{}{},
		"resilient-server-handshake": struct{}{},
		"resilient-server-signoff":   struct{}{}}

	if socket, err = zmq4.NewSocket(zmq4.REP); err != nil {
		return nil, err
	}

	fog.Info("%s binding socket to %s", logTag, endpoint)
	if err = socket.Bind(endpoint); err != nil {
		return nil, err
	}

	go func() {
		defer socket.Close()
		for {

			rawMessage, err := socket.RecvMessage(0)
			if err != nil {
				fog.Critical("RecvMessage failed %s", err)
			}

			var message Message
			err = json.Unmarshal([]byte(rawMessage[0]), &message)
			if err != nil {
				fog.Critical("Unmarshal failed %s", err)
			}

			reply := Message{
				"message-type":  "resilient-server-ack",
				"message-id":    message["message-id"],
				"incoming-type": message["message-type"],
				"accepted":      "true"}

			marshalledReply, err := json.Marshal(reply)
			if err != nil {
				fog.Critical("Marshal failed %s")
			}

			_, err = socket.SendMessage([]string{string(marshalledReply)})
			if err != nil {
				fog.Critical("SendMessage failed %s", err)
			}

			/* ---------------------------------------------------------------
			** 2014-05-21 dougfort:
			** The python version of ResiliantServer maintains a dict of
			** "client-address" for sending replies, but IMO we don't need
			** that here because every message contains "client-address"
			** so we can decouple the reply.
			** -------------------------------------------------------------*/
			_, ok := localMessageMap[message["message-type"]]
			if !ok {
				body := make([][]byte, len(rawMessage)-1)
				for i := 1; i < len(rawMessage); i++ {
					body[i-1] = []byte(rawMessage[i])
				}
				messageChan <- MessageWithBody{Message: message, Body: body}
			}
		}
	}()

	return messageChan, nil
}
