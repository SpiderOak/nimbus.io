package main

import (
	"log"
	"net/http"
)

var (
	requestIDKey = http.CanonicalHeaderKey("x-nimbus-io-user-request-id")
)

type handlerStruct struct {
}

// NewHandler returns an entity that implements the http.Handler interface
// this handles all incoming requests
func NewHandler() http.Handler {
	return &handlerStruct{}
}

// ServeHTTP implements the http.Handler interface
// handles all HTTP requests
func (h *handlerStruct) ServeHTTP(responseWriter http.ResponseWriter,
	request *http.Request) {
	var err error

	// Handle ping early so it doesn't clutter up the code
	if request.Method == "GET" && request.URL.Path == "/ping" {
		if _, err = responseWriter.Write([]byte("ok")); err != nil {
			log.Printf("error: ping responseWriter.Write %s", err)
		}
		return
	}

	requestID := request.Header.Get(requestIDKey)
	log.Printf("debug: %s method=%s, URL=%s from %s",
		requestID, request.Method, request.URL, request.RemoteAddr)

	http.NotFound(responseWriter, request)
}
