package reply

import (
	"encoding/json"
	"fmt"

	"github.com/pebbe/zmq4"

	"fog"

	"datawriter/msg"
)

const (
	replyChanCapacity = 100
	pushSocketSendHWM = 10
)

type ReplyMessage struct {
	ClientAddress string
	Content       map[string]interface{}
}

func NewReplyMessage(messageType, messageID, userRequestID string,
	returnAddress msg.ReturnAddress) *ReplyMessage {
	var replyMessage ReplyMessage

	replyMessage.ClientAddress = returnAddress.ClientAddress
	replyMessage.Content = make(map[string]interface{})

	replyMessage.Content["message-type"] = messageType + "-reply"
	replyMessage.Content["client-tag"] = returnAddress.ClientTag
	replyMessage.Content["client-address"] = returnAddress.ClientAddress
	replyMessage.Content["user-request-id"] = userRequestID
	replyMessage.Content["message-id"] = messageID
	replyMessage.Content["result"] = nil
	replyMessage.Content["error-message"] = nil

	return &replyMessage
}

func (replyMessage *ReplyMessage) Success() {
	replyMessage.Content["result"] = "success"
	replyMessage.Content["error-message"] = ""
}

func (replyMessage *ReplyMessage) Error(result, message string) {
	replyMessage.Content["result"] = result
	replyMessage.Content["error-message"] = message
}

func NewReplyHandler() chan<- *ReplyMessage {
	replyChan := make(chan *ReplyMessage, replyChanCapacity)
	pushSockets := make(map[string]*zmq4.Socket)

	go func() {
		for replyMessage := range replyChan {

			marshalledReply, err := json.Marshal(replyMessage.Content)
			if err != nil {
				fog.Error("unable to marshall reply %s %s", replyMessage, err)
				continue
			}

			var pushSocket *zmq4.Socket
			var ok bool
			pushSocket, ok = pushSockets[replyMessage.ClientAddress]
			if !ok {
				fog.Info("creating PUSH socket to %s", replyMessage.ClientAddress)
				if pushSocket, err = createPushSocket(); err != nil {
					fog.Error("Unable to create PUSH socket for %s %s",
						replyMessage.ClientAddress, err)
					continue
				}
				if err = pushSocket.Connect(replyMessage.ClientAddress); err != nil {
					fog.Error("Unable to Connect PUSH socket to %s %s",
						replyMessage.ClientAddress, err)
					pushSocket.Close()
					continue
				}
				pushSockets[replyMessage.ClientAddress] = pushSocket
			}

			if _, err = pushSocket.SendMessage(marshalledReply); err != nil {
				fog.Error("pushSocket SendMessage to %s failed %s",
					replyMessage.ClientAddress, err)
				pushSocket.Close()
				delete(pushSockets, replyMessage.ClientAddress)
				continue
			}
		}

	}()

	return replyChan
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
