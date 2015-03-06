package handler

import (
	"log"
	"net/http"

	"webwriter/req"
)

func AbortConjoined(responseWriter http.ResponseWriter,
	request *http.Request, parsedRequest req.ParsedRequest) {

	log.Printf("debug: %s; %s %s %s %d", parsedRequest.Type,
		parsedRequest.RequestID, parsedRequest.CollectionName,
		parsedRequest.Key, parsedRequest.UnifiedID)

	http.Error(responseWriter, "Not implemented",
		http.StatusInternalServerError)
}
