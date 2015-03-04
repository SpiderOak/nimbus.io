package main

import (
	"log"
	"net/http"
)

func archiveKey(responseWriter http.ResponseWriter,
	request *http.Request, parsedRequest ParsedRequest) {

	log.Printf("debug: %s; %s %s %s", parsedRequest.Type,
		parsedRequest.RequestID, parsedRequest.CollectionName,
		parsedRequest.Key)

	http.Error(responseWriter, "Not implemented",
		http.StatusInternalServerError)
}
