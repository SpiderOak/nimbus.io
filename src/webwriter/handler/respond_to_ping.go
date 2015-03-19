package handler

import (
	"fmt"
	"net/http"

	"tools"
	"types"
	"unifiedid"

	"webwriter/req"
	"webwriter/writers"
)

func RespondToPing(
	responseWriter http.ResponseWriter,
	_ *http.Request,
	_ req.ParsedRequest,
	_ types.CollectionRow,
	_ unifiedid.UnifiedIDChan,
	_ tools.Deliverator,
	_ []writers.DataWriterClientChan) error {

	if _, err := responseWriter.Write([]byte("ok")); err != nil {
		return fmt.Errorf("responseWriter.Write %s", err)
	}

	return nil
}
