package handler

import (
	"net/http"

	"types"
	"unifiedid"

	"webwriter/req"
	"webwriter/writers"
)

type RequestHandler func(
	http.ResponseWriter,
	*http.Request,
	req.ParsedRequest,
	types.CollectionRow,
	unifiedid.UnifiedIDChan,
	[]writers.DataWriterClientChan) error
