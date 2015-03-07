package handler

import (
	"fmt"
	"net/http"

	"types"

	"webwriter/req"
)

func RespondToPing(responseWriter http.ResponseWriter,
	_ *http.Request, parsedRequest req.ParsedRequest,
	collectionRow types.CollectionRow) error {

	if _, err := responseWriter.Write([]byte("ok")); err != nil {
		return fmt.Errorf("responseWriter.Write %s", err)
	}

	return nil
}
