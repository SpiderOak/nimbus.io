package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pebbe/zmq4"
)

type MessageChannel <-chan interface{}

const (
	pullSocketReceiveHWM   = 1024
	messageChannelCapacity = 100
)

var (
	pullSocketAddress = os.Getenv("NIMBUSIO_WEB_WRITER_PIPELINE_ADDRESS")
)

// NewPullSocketHandler returns a channel that will contain messages
// sent to the pull socket
func NewPullSocketHandler() MessageChannel {
	messageChannel := make(chan interface{}, messageChannelCapacity)

	go func() {
		var err error
		var zmqContext *zmq4.Context
		var pullSocket *zmq4.Socket

		if zmqContext, err = zmq4.NewContext(); err != nil {
			log.Fatalf("critical: pull socket NewContext %s", err)
		}

		if pullSocket, err = createPullSocket(); err != nil {
			log.Fatalf("critical: createPullSocket %s", err)
		}

		defer func() {
			pullSocket.Close()
			zmqContext.Term()
		}()

		log.Printf("info: binding pull socket to %s", pullSocketAddress)
		if err = pullSocket.Bind(pullSocketAddress); err != nil {
			log.Fatalf("critical: pull socket Bind(%s) %s",
				pullSocketAddress, err)
		}

		for {
			rawData, err := pullSocket.RecvBytes(0)
			if err != nil {
				// XXX: there are some errors we can recover from
				log.Fatalf("critical: pullSocket.RecvBytes() %s", err)
			}

			messageChannel <- rawData
		}
	}()

	return messageChannel
}

func createPullSocket(zmqContext *zmq4.Context) (*zmq4.Socket, error) {
	var err error
	var pullSocket *zmq4.Socket

	if pullSocket, err = zmqContext.NewSocket(zmq4.PULL); err != nil {
		return nil, fmt.Errorf("NewSocket PULL %s", err)
	}

	if err = pullSocket.SetRcvhwm(pullSocketReceiveHWM); err != nil {
		return nil, fmt.Errorf("pullSocket.SetRcvhwm(%d) %s",
			pullSocketReceiveHWM, err)
	}

	return pullSocket, nil
}
