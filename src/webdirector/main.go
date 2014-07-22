package main

import (
	"crypto/rand"
	"crypto/tls"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"fog"
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
				go handleConnection(conn)
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
	listenHost := os.Getenv("NIMBUSIO_WEB_DIRECTOR_INTERFACE")
	listenPort := os.Getenv("NIMBUSIO_WEB_DIRECTOR_PORT")
	return listenHost + ":" + listenPort, nil
}

func loadCertificate() (cert tls.Certificate, err error) {

	// TODO: need to find the real cert for nimbus.io

	keysDir := os.Getenv("SPIDEROAK_KEYS_DIR")
	keyPath := filepath.Join(keysDir, "privateKeyExample.pem")
	certPath := filepath.Join(keysDir, "certificateExample.pem")

	return tls.LoadX509KeyPair(certPath, keyPath)
}
