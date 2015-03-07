package handler

import (
	"fmt"
	"log"
	"net/http"

	"types"

	"webwriter/req"
)

func FinishConjoined(responseWriter http.ResponseWriter,
	request *http.Request, parsedRequest req.ParsedRequest,
	collectionRow types.CollectionRow) error {

	log.Printf("debug: %s; %s %s %s %d", parsedRequest.Type,
		parsedRequest.RequestID, parsedRequest.CollectionName,
		parsedRequest.Key, parsedRequest.UnifiedID)

	return fmt.Errorf("%s not Implemented", parsedRequest.Type)
}
