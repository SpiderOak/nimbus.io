package handler

import (
	"log"
	"net/http"
)

func respondToPing(responseWriter http.ResponseWriter,
	_ *http.Request, parsedRequest ParsedRequest) {

	if _, err := responseWriter.Write([]byte("ok")); err != nil {
		log.Printf("error: %s; error responseWriter.Write %s",
			parsedRequest.Type, err)
	}
}
