package main

import (
	"log"
	"net/http"
)

func respondToPing(responseWriter http.ResponseWriter,
	request *http.Request, parsedRequest ParsedRequest) {

	var err error

	if _, err = responseWriter.Write([]byte("ok")); err != nil {
		log.Printf("error: ping responseWriter.Write %s", err)
	}
}
