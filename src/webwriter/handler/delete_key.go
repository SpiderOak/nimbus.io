package handler

import (
	"fmt"
	"log"
	"net/http"

	"types"
	"unifiedid"

	"webwriter/req"
	"webwriter/writers"
)

func DeleteKey(
	responseWriter http.ResponseWriter,
	request *http.Request,
	parsedRequest req.ParsedRequest,
	collectionRow types.CollectionRow,
	unifiedIDChannel unifiedid.UnifiedIDChan,
	dataWriterClientChans []writers.DataWriterClientChan) error {

	log.Printf("debug: %s; %s %s %s", parsedRequest.Type,
		parsedRequest.RequestID, parsedRequest.CollectionName,
		parsedRequest.Key)

	return fmt.Errorf("%s not Implemented", parsedRequest.Type)
}
