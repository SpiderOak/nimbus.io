package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/pebbe/zmq4"

	"centraldb"
	"tools"
)

const (
	reactorPollingInterval = time.Second
	pullSocketReceiveHWM   = 1024
)

var (
	pullSocketAddress = os.Getenv("NIMBUSIO_WEB_WRITER_PIPELINE_ADDRESS")
)

// main entry point for webdirector
func main() {
	var err error
	var pullSocket *zmq4.Socket

	log.SetFlags(0) // suppress date/time: svlogd supplies that
	log.Printf("info: program starts")
	tools.SetMaxProcs()

	go func() {
		centralDB := centraldb.NewCentralDB()

		http.Handle("/", NewHandler(centralDB))
		err = http.ListenAndServe(getListenAddress(), nil)

		if err != nil {
			log.Printf("error: program terminates %s")
		} else {
			log.Printf("info: program terminates")
		}
	}()

	if pullSocket, err = createPullSocket(); err != nil {
		log.Fatalf("critical: createPullSocket %s", err)
	}
	defer pullSocket.Close()

	log.Printf("info: binding pull socket to %s", pullSocketAddress)
	if err = pullSocket.Bind(pullSocketAddress); err != nil {
		log.Fatalf("critical: Bind(%s) %s", pullSocketAddress, err)
	}

	reactor := zmq4.NewReactor()
	//	reactor.SetVerbose(true)

	reactor.AddSocket(pullSocket, zmq4.POLLIN,
		NewPullSocketHandler(pullSocket))

	err = reactor.Run(reactorPollingInterval)
	if err == tools.SigtermError {
		log.Printf("info: program terminates normally due to SIGTERM")
	} else if errno, ok := err.(syscall.Errno); ok {
		// we can get 'interrupted system call' if we get SIGTERM while
		// a socket is waiting on a read. That's not too bad.
		if errno == syscall.EINTR {
			log.Printf("warning: reactor.Run returns '%s' assuming SIGTERM",
				errno)
		} else {
			log.Printf("error: reactor.Run returns %T '%s'", errno, errno)
		}
	} else {
		log.Printf("error: reactor.Run returns %T %s", err, err)
	}
}

func createPullSocket() (*zmq4.Socket, error) {
	var err error
	var pullSocket *zmq4.Socket

	if pullSocket, err = zmq4.NewSocket(zmq4.PULL); err != nil {
		return nil, fmt.Errorf("NewSocket PULL %s", err)
	}

	if err = pullSocket.SetRcvhwm(pullSocketReceiveHWM); err != nil {
		return nil, fmt.Errorf("pullSocket.SetRcvhwm(%d) %s",
			pullSocketReceiveHWM, err)
	}

	return pullSocket, nil
}

func getListenAddress() string {
	// set up the listener port
	listenHost := os.Getenv("NIMBUSIO_WEB_WRITER_HOST")
	listenPort := os.Getenv("NIMBUSIO_WEB_WRITER_PORT")

	log.Printf("info: NIMBUSIO_WEB_WRITER_HOST = '%s', NIMBUSIO_WEB_WRITER_PORT = '%s'",
		listenHost, listenPort)

	return listenHost + ":" + listenPort
}
