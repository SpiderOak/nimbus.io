package main

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/pebbe/zmq4"

	"fog"
	"tools"
)

const (
	signalChannelCapacity  = 1
	writerSocketReceiveHWM = 100
	writerSocketSendHWM    = 10
	reactorPollingInterval = time.Second
)

var (
	dataWriterAddress         = os.Getenv("NIMBUSIO_DATA_WRITER_ADDRESS")
	eventAggregatorPubAddress = os.Getenv("NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS")
)

// main entry point for data writer
func main() {
	var err error
	var writerSocket *zmq4.Socket
	var eventSubSocket *zmq4.Socket

	fog.Info("program starts")
	tools.SetMaxProcs()

	if writerSocket, err = createWriterSocket(); err != nil {
		fog.Critical("createWriterSocket %s", err)
	}
	defer writerSocket.Close()

	fog.Info("binding writer socket to %s", dataWriterAddress)
	if err = writerSocket.Bind(dataWriterAddress); err != nil {
		fog.Critical("Bind(%s) %s", dataWriterAddress, err)
	}

	if eventSubSocket, err = createEventSubSocket(); err != nil {
		fog.Critical("createEventSubSocket %s", err)
	}
	defer eventSubSocket.Close()

	fog.Info("connecting event sub socket to %s", eventAggregatorPubAddress)
	if err = eventSubSocket.Connect(eventAggregatorPubAddress); err != nil {
		fog.Critical("Connect(%s) %s", eventAggregatorPubAddress, err)
	}

	messageChan := NewMessageHandler()

	reactor := zmq4.NewReactor()
	reactor.AddChannel(tools.NewSignalWatcher(), 1, tools.SigtermHandler)
	reactor.AddSocket(writerSocket, zmq4.POLLIN,
		NewWriterSocketHandler(writerSocket, messageChan))
	reactor.AddSocket(eventSubSocket, zmq4.POLLIN,
		NewEventSubSocketHandler(eventSubSocket))

	fog.Debug("starting reactor.Run")
	reactor.SetVerbose(true)
	err = reactor.Run(reactorPollingInterval)
	if err == tools.SigtermError {
		fog.Info("program terminates normally due to SIGTERM")
	} else if errno, ok := err.(syscall.Errno); ok {
		// we can get 'interrupted system call' if we get SIGTERM while
		// a socket is waiting on a read. That's not too bad.
		if errno == syscall.EINTR {
			fog.Warn("reactor.Run returns '%s' assuming SIGTERM", errno)
		} else {
			fog.Error("reactor.Run returns %T '%s'", errno, errno)
		}
	} else {
		fog.Error("reactor.Run returns %T %s", err, err)
	}
}

func createWriterSocket() (*zmq4.Socket, error) {
	var err error
	var writerSocket *zmq4.Socket

	if writerSocket, err = zmq4.NewSocket(zmq4.REP); err != nil {
		return nil, fmt.Errorf("NewSocket %s", err)
	}

	if err = writerSocket.SetRcvhwm(writerSocketReceiveHWM); err != nil {
		return nil, fmt.Errorf("writerSocket.SetRcvhwm(%d) %s",
			writerSocketReceiveHWM, err)
	}

	if err = writerSocket.SetSndhwm(writerSocketSendHWM); err != nil {
		return nil, fmt.Errorf("writerSocket.SetSndhwm(%d) %s",
			writerSocketSendHWM, err)
	}

	return writerSocket, nil
}

func createEventSubSocket() (*zmq4.Socket, error) {
	var err error
	var subSocket *zmq4.Socket

	if subSocket, err = zmq4.NewSocket(zmq4.SUB); err != nil {
		return nil, fmt.Errorf("NewSocket %s", err)
	}

	if err = subSocket.SetSubscribe("web-writer-start"); err != nil {
		return nil, fmt.Errorf("SetSubscribe %s", err)
	}

	return subSocket, nil
}
