package handler

import (
	"log"
	"net/http"

	"webwriter/req"
)

func RespondToPing(responseWriter http.ResponseWriter,
	_ *http.Request, parsedRequest req.ParsedRequest) {

	if _, err := responseWriter.Write([]byte("ok")); err != nil {
		log.Printf("error: %s; error responseWriter.Write %s",
			parsedRequest.Type, err)
	}
}
