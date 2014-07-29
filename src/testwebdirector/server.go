package main

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"fog"
)

type Server interface {
	Serve()
}

type serverImpl struct {
	Name    string
	Address string
}

func NewServer(name, address string) Server {
	return serverImpl{Name: name, Address: address}
}

func (s serverImpl) Serve() {
	fog.Info("(%s) listening to %s", s.Name, s.Address)
	http.HandleFunc(fmt.Sprintf("/%s", s.Name), s.handleAll)
	http.ListenAndServe(s.Address, nil)
}

func (s serverImpl) handleAll(w http.ResponseWriter, req *http.Request) {
	fog.Debug("(%s) got request %s %s", s.Name, req.Method, req.URL)
	if req.Body != nil {
		fog.Debug("(%s) sleepinjg 10 seconds", s.Name)
		time.Sleep(10 * time.Second)
		s.readAndDiscard(req.Body)
		req.Body.Close()
	}
	io.WriteString(w, "hello, world!\n")
}

func (s serverImpl) readAndDiscard(reader io.Reader) {
	var bytesRead uint64
	bufferSize := 64 * 1024

	for true {
		buffer := make([]byte, bufferSize, bufferSize)
		n, err := reader.Read(buffer)
		bytesRead += uint64(n)
		if err != nil {
			fog.Debug("(%s) %s %d total bytes read", s.Name, err, bytesRead)
			break
		}
		fog.Debug("(%s) read %d bytes", s.Name, n)
	}
}
