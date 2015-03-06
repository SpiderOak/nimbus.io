package handler

import (
	"log"
	"net/http"

	"webwriter/req"
)

func DeleteKey(responseWriter http.ResponseWriter,
	request *http.Request, parsedRequest req.ParsedRequest) {

	log.Printf("debug: %s; %s %s %s", parsedRequest.Type,
		parsedRequest.RequestID, parsedRequest.CollectionName,
		parsedRequest.Key)

	http.Error(responseWriter, "Not implemented",
		http.StatusInternalServerError)
}
