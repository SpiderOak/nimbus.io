package main

import (
	"crypto/rand"
	"crypto/tls"
	"net"
	"os"
	"os/signal"
	"syscall"

	"fog"

	"webdirector/avail"
	"webdirector/hosts"
	"webdirector/mgmtapi"
	"webdirector/routing"
)

const (
	listenerChanCapacity = 100
)

// amin entry point for webdirector
func main() {
	var err error

	fog.Info("program starts")

	// set up a signal handling channel
	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	listenAddress, err := getListenAddress()
	if err != nil {
		fog.Critical("error getListenAddress %s", err)
	}
	fog.Info("webdirector listens to %s", listenAddress)

	cert, err := loadCertificate()
	if err != nil {
		fog.Critical("unable to load certificate %s", err)
	}
	config := tls.Config{Certificates: []tls.Certificate{cert}}
	config.Rand = rand.Reader

	listener, err := tls.Listen("tcp", listenAddress, &config)
	if err != nil {
		fog.Critical("tls.Listen %s failed %s", listenAddress, err)
	}

	managmentAPIDests, err := mgmtapi.NewManagementAPIDestinations()
	if err != nil {
		fog.Critical("NewManagementAPIDestinations: %s", err)
	}

	hostsForCollection, err := hosts.NewHostsForCollection()
	if err != nil {
		fog.Critical("NewHostsForCollection: %s", err)
	}
	availableHosts := avail.NewAvailability()

	router := routing.NewRouter(managmentAPIDests, hostsForCollection, availableHosts)

	fog.Info("NIMBUS_IO_SERVICE_DOMAIN = '%s'",
		os.Getenv("NIMBUS_IO_SERVICE_DOMAIN"))
	fog.Info("NIMBUSIO_WEB_PUBLIC_READER_PORT = '%s'",
		os.Getenv("NIMBUSIO_WEB_PUBLIC_READER_PORT"))
	fog.Info("NIMBUSIO_WEB_WRITER_PORT = '%s'",
		os.Getenv("NIMBUSIO_WEB_WRITER_PORT"))

	listenerChan := make(chan net.Conn, listenerChanCapacity)
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				fog.Error("listener.Accept() %s", err)
				close(listenerChan)
				break
			}
			listenerChan <- connection
		}
	}()

	for running := true; running; {
		select {
		case signal := <-signalChannel:
			fog.Info("terminated by signal: %v", signal)
			running = false
		case conn, ok := <-listenerChan:
			if ok {
				fog.Info("connection from %s", conn.RemoteAddr().String())
				go handleConnection(router, conn)
			} else {
				running = false
			}
		}
	}
	listener.Close()

	fog.Info("program terminates")
}

func getListenAddress() (string, error) {
	// set up the listener port
	listenHost := os.Getenv("NIMBUSIO_WEB_DIRECTOR_ADDR")
	listenPort := os.Getenv("NIMBUSIO_WEB_DIRECTOR_PORT")

	fog.Info("NIMBUSIO_WEB_DIRECTOR_ADDR = '%s', NIMBUSIO_WEB_DIRECTOR_PORT = '%s'",
		listenHost, listenPort)

	return listenHost + ":" + listenPort, nil
}

func loadCertificate() (cert tls.Certificate, err error) {
	certPath := os.Getenv("NIMBUSIO_WILDCARD_SSL_CERT")
	keyPath := os.Getenv("NIMBUSIO_WILDCARD_SSL_KEY")

	fog.Info("LoadX509KeyPair: certPath = '%s', keyPath = '%s'",
		certPath, keyPath)

	return tls.LoadX509KeyPair(certPath, keyPath)
}
