package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"strconv"

	"tools"

	"webwriter/writers"
)

// main entry point for webdirector
func main() {
	var err error
	var listenAddress net.TCPAddr
	var listener *net.TCPListener
	var dataWriterClientChans []writers.DataWriterClientChan
	var handler http.Handler

	log.SetFlags(0) // suppress date/time: svlogd supplies that
	log.Printf("info: program starts")
	tools.SetMaxProcs()

	_ = NewPullSocketHandler()

	if dataWriterClientChans, err = writers.NewDataWriterClients(); err != nil {
		log.Fatalf("critical: NewDataWriterClients failed %s", err)
	}

	if listenAddress, err = getListenAddress(); err != nil {
		log.Fatalf("critical: listen address %s", err)
	}

	log.Printf("info: listening for HTTP on %s", listenAddress)

	if listener, err = net.ListenTCP("tcp", &listenAddress); err != nil {
		log.Fatalf("critical: ListenTCP %s", err)
	}

	if handler, err = NewHandler(dataWriterClientChans); err != nil {
		log.Fatalf("critical: NewHandler %s", err)
	}
	http.Handle("/", handler)

	err = http.Serve(listener, nil)

	if err != nil {
		log.Printf("error: program terminates %s")
	} else {
		log.Printf("info: program terminates")
	}
}

func getListenAddress() (net.TCPAddr, error) {
	var listenAddress net.TCPAddr
	var err error

	listenAddress.IP = net.ParseIP(os.Getenv("NIMBUSIO_WEB_WRITER_HOST"))
	listenAddress.Port, err = strconv.Atoi(os.Getenv("NIMBUSIO_WEB_WRITER_PORT"))

	return listenAddress, err
}
