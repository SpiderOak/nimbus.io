package handler

import (
	"net/http"

	"tools"
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
	tools.Deliverator,
	[]writers.DataWriterClientChan) error
