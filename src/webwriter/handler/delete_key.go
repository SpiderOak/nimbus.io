package handler

import (
	"fmt"
	"log"
	"net/http"

	"types"

	"webwriter/req"
)

func DeleteKey(responseWriter http.ResponseWriter,
	request *http.Request, parsedRequest req.ParsedRequest,
	collectionRow types.CollectionRow) error {

	log.Printf("debug: %s; %s %s %s", parsedRequest.Type,
		parsedRequest.RequestID, parsedRequest.CollectionName,
		parsedRequest.Key)

	return fmt.Errorf("%s not Implemented", parsedRequest.Type)
}
