package main

import (
	"log"
	"net/http"

	"auth"
	"centraldb"
	"types"

	"webwriter/handler"
	"webwriter/req"
)

type handlerEntry struct {
	Func   handler.RequestHandler
	Access auth.AccessType
}

type handlerStruct struct {
	CentralDB centraldb.CentralDB
	Dispatch  map[req.RequestType]handlerEntry
}

// NewHandler returns an entity that implements the http.Handler interface
// this handles all incoming requests
func NewHandler(centralDB centraldb.CentralDB) http.Handler {
	h := handlerStruct{CentralDB: centralDB}
	h.Dispatch = map[req.RequestType]handlerEntry{
		req.RespondToPing: handlerEntry{Func: handler.RespondToPing,
			Access: auth.None},
		req.ArchiveKey: handlerEntry{Func: handler.ArchiveKey,
			Access: auth.Write},
		req.DeleteKey: handlerEntry{Func: handler.DeleteKey,
			Access: auth.Delete},
		req.StartConjoined: handlerEntry{Func: handler.StartConjoined,
			Access: auth.Write},
		req.FinishConjoined: handlerEntry{Func: handler.FinishConjoined,
			Access: auth.Write},
		req.AbortConjoined: handlerEntry{Func: handler.AbortConjoined,
			Access: auth.Write}}
	return &h
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
	var parsedRequest req.ParsedRequest
	var collectionRow types.CollectionRow

	if parsedRequest, err = req.ParseRequest(request); err != nil {
		log.Printf("error: unparsable request: %s, method='%s'", err,
			request.Method)
		http.Error(responseWriter, "unparsable request", http.StatusBadRequest)
		return
	}

	if request.URL.Path != "/ping" {
		log.Printf("debug: %s method=%s, collection=%s path=%s query=%s %s",
			parsedRequest.RequestID, request.Method,
			parsedRequest.CollectionName, request.URL.Path,
			request.URL.RawQuery, request.RemoteAddr)
	}

	dispatchEntry, ok := h.Dispatch[parsedRequest.Type]
	if !ok {
		// this shouldn't happen
		log.Printf("error: unknown request type: %s", parsedRequest.Type)
		http.Error(responseWriter, "unknown request type",
			http.StatusInternalServerError)
		return
	}

	if dispatchEntry.Access == auth.None {
		err = dispatchEntry.Func(responseWriter, request, parsedRequest,
			types.CollectionRow{})
		if err != nil {
			log.Printf("error: ping %s", err)
		}
		return
	}

	collectionRow, err = h.CentralDB.GetCollectionRow(
		parsedRequest.CollectionName)
	if err != nil {
		log.Printf("error: unknown collection: %s",
			parsedRequest.CollectionName)
		http.Error(responseWriter, "unknown collection", http.StatusNotFound)
		return
	}

	err = dispatchEntry.Func(responseWriter, request, parsedRequest,
		collectionRow)
	if err != nil {
		log.Printf("error: %s handler failed: %s", parsedRequest.Type, err)
		http.Error(responseWriter, "handler failed",
			http.StatusInternalServerError)
		return
	}
}
