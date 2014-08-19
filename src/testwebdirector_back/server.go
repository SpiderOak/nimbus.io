package main

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"fog"
	"tools"
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
	switch req.Method {
	case "GET":
		s.handleGET(w, req)
	case "POST":
		s.handlePOST(w, req)
	default:
		fog.Error("(%s) unknown method %s %s", s.Name, req.Method, req.URL)
	}
}

func (s serverImpl) handleGET(w http.ResponseWriter, req *http.Request) {
	fog.Debug("(%s) got request %s %s", s.Name, req.Method, req.URL)
	if req.Body != nil {
		tools.ReadAndDiscard(req.Body)
		req.Body.Close()
	}

	var contentLength uint64 = 1024 * 1024 * 1024
	fog.Debug("response body %dmb", contentLength/(1024*1024))
	bodyReader := tools.NewSizeReader(contentLength)

	http.ServeContent(w, req, "content.txt", time.Now(), bodyReader)
}

func (s serverImpl) handlePOST(w http.ResponseWriter, req *http.Request) {
	fog.Debug("(%s) got request %s %s", s.Name, req.Method, req.URL)
	if req.Body != nil {
		tools.ReadAndDiscard(req.Body)
		req.Body.Close()
	}
	io.WriteString(w, "hello, world!\n")
}
