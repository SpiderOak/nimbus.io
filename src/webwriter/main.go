package main

import (
	"log"
	"net/http"
	"os"

	"centraldb"
)

// main entry point for webdirector
func main() {
	var err error

	log.SetFlags(0) // suppress date/time: svlogd supplies that
	log.Printf("info: program starts")

	centralDB := centraldb.NewCentralDB()

	http.Handle("/", NewHandler(centralDB))
	err = http.ListenAndServe(getListenAddress(), nil)

	if err != nil {
		log.Printf("error: program terminates %s")
	} else {
		log.Printf("info: program terminates")
	}
}

func getListenAddress() string {
	// set up the listener port
	listenHost := os.Getenv("NIMBUSIO_WEB_WRITER_HOST")
	listenPort := os.Getenv("NIMBUSIO_WEB_WRITER_PORT")

	log.Printf("info: NIMBUSIO_WEB_WRITER_HOST = '%s', NIMBUSIO_WEB_WRITER_PORT = '%s'",
		listenHost, listenPort)

	return listenHost + ":" + listenPort
}
