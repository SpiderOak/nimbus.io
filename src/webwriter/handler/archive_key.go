package handler

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	"types"

	"webwriter/req"
)

var (
	contentLengthKey = http.CanonicalHeaderKey("content-length")
	contentMD5Key    = http.CanonicalHeaderKey("content-md5")
)

func ArchiveKey(responseWriter http.ResponseWriter,
	request *http.Request, parsedRequest req.ParsedRequest,
	collectionRow types.CollectionRow) error {

	var err error
	var contentLength int

	if contentLength, err = getContentLength(request); err != nil {
		return err
	}

	log.Printf("debug: %s; %s %s %s; content-length=%d, content-md5=%s",
		parsedRequest.Type,
		parsedRequest.RequestID,
		parsedRequest.CollectionName,
		parsedRequest.Key,
		contentLength,
		request.Header.Get(contentMD5Key),
	)

	return fmt.Errorf("%s not Implemented", parsedRequest.Type)
}

func getContentLength(request *http.Request) (int, error) {
	contentLengthStr := request.Header.Get(contentLengthKey)
	if contentLengthStr == "" {
		return 0, fmt.Errorf("no content-length header")
	}

	return strconv.Atoi(contentLengthStr)
}
