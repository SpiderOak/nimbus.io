package handler

import (
	"net/http"

	"types"

	"webwriter/req"
)

type RequestHandler func(http.ResponseWriter, *http.Request,
	req.ParsedRequest, types.CollectionRow) error
