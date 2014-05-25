package main

import (
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/pebbe/zmq4"

	"fog"
	"tools"
)

const (
	signalChannelCapacity  = 1
	reactorPollingInterval = time.Second
)

var (
	dataWriterAddress = os.Getenv("NIMBUSIO_DATA_WRITER_ADDRESS")
)

func init() {
	// Issue #2
	// if max procs is specfied in the environment, leave it
	// otherwise set it to one less than the number of available cores
	maxProcsStr := os.Getenv("GOMAXPROCS")
	if maxProcsStr == "" {
		maxProcs := runtime.NumCPU() - 1
		if maxProcs < 1 {
			maxProcs = 1
		}
		fog.Info("setting GOMAXPROCS to %d internally", maxProcs)
		runtime.GOMAXPROCS(maxProcs)
	} else {
		fog.Info("GOMAXPROCS set to %s in environment", maxProcsStr)
	}
}

// main entry point for data writer
func main() {
	var err error
	var writerSocket *zmq4.Socket

	fog.Info("program starts %s", tools.GetZMQVersion())

	if writerSocket, err = zmq4.NewSocket(zmq4.REP); err != nil {
		fog.Critical("NewSocket %s", err)
	}
	defer writerSocket.Close()

	fog.Info("binding writer socket to %s", dataWriterAddress)
	if err = writerSocket.Bind(dataWriterAddress); err != nil {
		fog.Critical("Bind(%s) %s", dataWriterAddress, err)
	}

	messageChan := NewMessageHandler()

	reactor := zmq4.NewReactor()
	reactor.AddChannel(tools.NewSignalWatcher(), 1, tools.SigtermHandler)
	reactor.AddSocket(writerSocket, zmq4.POLLIN,
		NewWriterSocketHandler(writerSocket, messageChan))

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
