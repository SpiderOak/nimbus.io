package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pebbe/zmq4"

	"tools"
	"writermsg"
)

type DataWritersChan chan<- []byte

const (
	dataWritersChanCapacity      = 100
	dataWriterClientChanCapacity = 1
	clientSocketReceiveHWM       = 1
	clientSocketSendHWM          = 1
)

var (
	nodeNames              = strings.Split(os.Getenv("NIMBUSIO_NODE_NAME_SEQ"), " ")
	dataWriterAddresses    = strings.Split(os.Getenv("NIMBUSIO_DATA_WRITER_ADDRESSES"), " ")
	ackTimeout             time.Duration
	handshakeRetryInterval time.Duration
	maxIdleTime            time.Duration
	reportingInterval      time.Duration
	connectDelay           time.Duration
	clientTag              string
)

// NewDataWriterClients returns a channel for sending data to all data writers
func NewDataWriterClients() (DataWritersChan, error) {
	var err error
	dataWritersChan := make(chan []byte, dataWritersChanCapacity)
	dataWriterClientChans := make([]DataWriterClientChan, len(nodeNames))

	if ackTimeout, err = getAckTimeout(); err != nil {
		return nil, err
	}
	handshakeRetryInterval = time.Minute
	maxIdleTime = time.Minute * 10
	reportingInterval = time.Minute
	connectDelay = time.Minute
	clientTag = fmt.Sprintf("web-writer-%s", os.Getenv("NIMBUSIO_NODE_NAME"))

	for i, nodeName := range nodeNames {
		dataWriterClientChans[i], err = NewDataWriterClient(nodeName,
			dataWriterAddresses[i])
		if err != nil {
			return nil, fmt.Errorf("NewDataWriterClient #%d %s %s %s",
				i+1, nodeName, dataWriterAddresses[i], err)
		}
	}

	go func() {
		for message := range dataWritersChan {
			for _, dataWriterClientChan := range dataWriterClientChans {
				dataWriterClientChan <- message
			}
		}

		for _, dataWriterClientChan := range dataWriterClientChans {
			close(dataWriterClientChan)
		}
	}()

	return dataWritersChan, nil
}

func getAckTimeout() (time.Duration, error) {
	ackTimeoutStr := os.Getenv("NIMBUSIO_ACK_TIMEOUT")
	if ackTimeoutStr == "" {
		ackTimeoutStr = "60"
	}
	ackTimeoutInt, err := strconv.Atoi(ackTimeoutStr)
	return time.Second * time.Duration(ackTimeoutInt), err
}

type DataWriterClientChan chan<- []byte

func NewDataWriterClient(nodeName, address string) (DataWriterClientChan, error) {
	var err error
	var zmqContext *zmq4.Context

	dataWriterClientChan := make(chan []byte, dataWriterClientChanCapacity)

	if zmqContext, err = zmq4.NewContext(); err != nil {
		return dataWriterClientChan, fmt.Errorf("%s NewContext %s",
			nodeName, err)
	}

	go func() {
		var err error
		var clientSocket *zmq4.Socket

		for message := range dataWriterClientChan {
			messageSent := false
		SEND_LOOP:
			for !messageSent {
				if clientSocket == nil {
					clientSocket, err = connectClientSocket(zmqContext, address)
					if err != nil {
						log.Printf("error: %s; %s", err)
						log.Printf("info: %s; retry in %s", nodeName,
							connectDelay)
						time.Sleep(connectDelay)
						continue SEND_LOOP
					}
				}

				if _, err = clientSocket.SendBytes(message, 0); err != nil {
					log.Printf("error: %s; SendBytes %s", nodeName, err)
					log.Printf("info: %s; retry in %s", nodeName, connectDelay)
					clientSocket.Close()
					time.Sleep(connectDelay)
					continue SEND_LOOP
				}

				messageSent = true
			}
		}
	}()

	return dataWriterClientChan, nil
}

func connectClientSocket(zmqContext *zmq4.Context, address string) (*zmq4.Socket, error) {

	var err error
	var clientSocket *zmq4.Socket
	var uuid string
	var marshaledMessage []byte
	var marshaledReply []byte
	var accepted bool

	if clientSocket, err = zmqContext.NewSocket(zmq4.REQ); err != nil {
		return nil, fmt.Errorf("NewSocket(zmq4.REQ) %s", err)
	}

	defer func() {
		if err != nil {
			clientSocket.Close()
		}
	}()

	if err = clientSocket.SetSndhwm(clientSocketSendHWM); err != nil {
		return nil, fmt.Errorf("SetSndhwm(%d) %s", clientSocketSendHWM, err)
	}

	if err = clientSocket.SetRcvhwm(clientSocketReceiveHWM); err != nil {
		return nil, fmt.Errorf("SetRcvhwm(%d) %s", clientSocketReceiveHWM, err)
	}

	if uuid, err = tools.CreateUUID(); err != nil {
		return nil, err
	}

	message := map[string]interface{}{
		"message-type":   "resilient-server-handshake",
		"message-id":     uuid,
		"client-tag":     clientTag,
		"client-address": pullSocketAddress,
	}

	if marshaledMessage, err = json.Marshal(message); err != nil {
		return nil, err
	}

	if _, err = clientSocket.SendBytes(marshaledMessage, 0); err != nil {
		return nil, err
	}

	if marshaledReply, err = clientSocket.RecvBytes(0); err != nil {
		return nil, err
	}

	if accepted, err = writermsg.GetAckAccepted(string(marshaledReply)); err != nil {
		return nil, err
	}

	if !accepted {
		return nil, fmt.Errorf("handshake not accepted")
	}

	return clientSocket, nil
}
