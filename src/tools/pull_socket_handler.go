package tools

import (
	"fmt"
	"log"

	"github.com/pebbe/zmq4"
)

type MessageChannel <-chan interface{}

const (
	pullSocketReceiveHWM   = 1024
	messageChannelCapacity = 100
)

// NewPullSocketHandler returns a channel that will contain messages
// sent to the pull socket
func NewPullSocketHandler(deliverator Deliverator, pullSocketAddress string) error {
	messageChannel := make(chan interface{}, messageChannelCapacity)

	var err error
	var zmqContext *zmq4.Context
	var pullSocket *zmq4.Socket

	if zmqContext, err = zmq4.NewContext(); err != nil {
		return fmt.Errorf("pull socket NewContext %s", err)
	}

	if pullSocket, err = zmqContext.NewSocket(zmq4.PULL); err != nil {
		return fmt.Errorf("NewSocket PULL %s", err)
	}

	if err = pullSocket.SetRcvhwm(pullSocketReceiveHWM); err != nil {
		return fmt.Errorf("pullSocket.SetRcvhwm(%d) %s",
			pullSocketReceiveHWM, err)
	}

	log.Printf("info: binding pull socket to %s", pullSocketAddress)
	if err = pullSocket.Bind(pullSocketAddress); err != nil {
		return fmt.Errorf("pull socket Bind(%s) %s",
			pullSocketAddress, err)
	}

	go func() {

		defer func() {
			pullSocket.Close()
			zmqContext.Term()
		}()

		for {
			rawData, err := pullSocket.RecvBytes(0)
			if err != nil {
				// XXX: there are some errors we can recover from
				log.Fatalf("critical: pullSocket.RecvBytes() %s", err)
			}

			messageChannel <- rawData
		}
	}()

	return nil
}
