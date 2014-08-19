package main

import (
	"fmt"
	"io"
	"net/http"

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
		s.readAndDiscard(req.Body)
		req.Body.Close()
	}
	io.WriteString(w, "hello, world!\n")
}

func (s serverImpl) readAndDiscard(reader io.Reader) {
	var bytesRead uint64
	var mbReported uint64
	bufferSize := 64 * 1024
	buffer := make([]byte, bufferSize, bufferSize)

	for true {
		n, err := reader.Read(buffer)
		bytesRead += uint64(n)
		if err != nil {
			fog.Debug("(%s) %s %d total bytes read", s.Name, err, bytesRead)
			break
		}
		mbRead := bytesRead / (1024 * 1024)
		if mbRead > mbReported {
			fog.Debug("(%s) read %dmb", s.Name, mbRead)
			mbReported = mbRead
		}
	}
}
