package main

import (
	"log"
	"net/http"
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

// https://<collection name>.nimbus.io/data/<key>
// https://<collection name>.nimbus.io/data/<key>?action=delete
// https://<collection name>.nimbus.io/conjoined/<key>?action=start
// https://<collection name>.nimbus.io/conjoined/<key>?action=finish&conjoined_identifier=<conjoined_identifier>
// https://<collection name>.nimbus.io/conjoined/<key>?action=abort&conjoined_identifier=<conjoined_identifier>

func (h *handlerStruct) ServeHTTP(responseWriter http.ResponseWriter,
	request *http.Request) {
	var err error
	var parsedRequest ParsedRequest

	if parsedRequest, err = parseRequest(request); err != nil {
		log.Printf("error: unparsable request: %s", err)
		http.Error(responseWriter, "unparsable request", http.StatusBadRequest)
		return
	}

	log.Printf("debug: %s method=%s, collection=%s path=%s query=%s fragment=%s URI=%s %s",
		parsedRequest.RequestID, request.Method, parsedRequest.CollectionName,
		request.URL.Path, request.URL.RawQuery, request.URL.Fragment,
		request.RequestURI, request.RemoteAddr)

	http.NotFound(responseWriter, request)
}
