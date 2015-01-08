package tools

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

const (
	signalChannelCapacity = 1
)

var (
	SigtermError = fmt.Errorf("SIGTERM")
)

// NewSignalWatcher waits for signal.Notify and pushes struct{} onto the
// chan. This is suitable for use with zmq.Reactor
func NewSignalWatcher() <-chan interface{} {
	// set up a channel that zmq.Reactor will like
	outputChannel := make(chan interface{}, signalChannelCapacity)

	// set up a signal handling channel
	signalChannel := make(chan os.Signal, signalChannelCapacity)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	go func() {
		// Block until we get a signal from the os
		<-signalChannel

		outputChannel <- struct{}{}
	}()

	return outputChannel
}

// SigtermHandler works with zmq.Reactor, when called it returns SigtermError
func SigtermHandler(_ interface{}) error {
	return SigtermError
}
