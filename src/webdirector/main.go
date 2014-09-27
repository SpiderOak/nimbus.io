package main

import (
	"crypto/rand"
	"crypto/tls"
	"net"
	"os"
	"os/signal"
	"syscall"

	"centraldb"
	"fog"

	"webdirector/avail"
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

	useTLS := os.Getenv("NIMBUS_IO_SERVICE_SSL") == "1"
	fog.Info("TLS = %t", useTLS)

	listenAddress, err := getListenAddress()
	if err != nil {
		fog.Critical("error getListenAddress %s", err)
	}
	fog.Info("webdirector listens to %s", listenAddress)

	var listener net.Listener
	if useTLS {
		if listener, err = getTLSListener(listenAddress); err != nil {
			fog.Critical("Unable to create TLS listener: %s", err)
		}
	} else {
		if listener, err = getTCPListener(listenAddress); err != nil {
			fog.Critical("Unable to create TCP listener: %s", err)
		}
	}

	managmentAPIDests, err := mgmtapi.NewManagementAPIDestinations()
	if err != nil {
		fog.Critical("NewManagementAPIDestinations: %s", err)
	}

	centralDB := centraldb.NewCentralDB()

	availableHosts, err := avail.NewAvailability()
	if err != nil {
		fog.Critical("NewAvailability: %s", err)
	}

	router := routing.NewRouter(managmentAPIDests, centralDB, availableHosts)

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

func getTLSListener(listenAddress string) (net.Listener, error) {
	cert, err := loadCertificate()
	if err != nil {
		return nil, err
	}
	config := tls.Config{Certificates: []tls.Certificate{cert}}
	config.Rand = rand.Reader

	return tls.Listen("tcp", listenAddress, &config)
}

func loadCertificate() (cert tls.Certificate, err error) {
	certPath := os.Getenv("NIMBUSIO_WILDCARD_SSL_CERT")
	keyPath := os.Getenv("NIMBUSIO_WILDCARD_SSL_KEY")

	fog.Info("LoadX509KeyPair: certPath = '%s', keyPath = '%s'",
		certPath, keyPath)

	return tls.LoadX509KeyPair(certPath, keyPath)
}

func getTCPListener(listenAddress string) (net.Listener, error) {
	return net.Listen("tcp", listenAddress)
}
