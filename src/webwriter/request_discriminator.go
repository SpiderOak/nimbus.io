package main

import (
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strings"
)

type RequestType uint8

const (
	_                         = iota
	RespondToPing RequestType = iota
	ArchiveKey
	DeleteKey
	StartConjoined
	FinishConjoined
	AbortConjoined
)

type ParsedRequest struct {
	Type           RequestType
	RequestID      string
	CollectionName string
	Key            string
	UnifiedID      uint64
}

var (
	requestIDKey          = http.CanonicalHeaderKey("x-nimbus-io-user-request-id")
	validCollectionRegexp = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)
	validKeyRegexp        = regexp.MustCompile(`^\S+$`)
)

func (r RequestType) String() string {
	switch r {
	case RespondToPing:
		return "RespondToPing"
	case ArchiveKey:
		return "ArchiveKey"
	case DeleteKey:
		return "DeleteKey"
	case StartConjoined:
		return "StartConjoined"
	case FinishConjoined:
		return "FinishConjoined"
	case AbortConjoined:
		return "AbortConjoined"
	default:
		return fmt.Sprintf("unknown request (%d)", int64(r))
	}
}

func parseRequest(request *http.Request) (ParsedRequest, error) {
	var parsedRequest ParsedRequest
	var err error

	upperCaseMethod := strings.ToUpper(request.Method)

	if upperCaseMethod == "GET" && request.URL.Path == "/ping" {
		parsedRequest.Type = RespondToPing
		return parsedRequest, nil
	}

	parsedRequest.RequestID = request.Header.Get(requestIDKey)

	if parsedRequest.CollectionName, err = parseCollectionName(request.Host); err != nil {
		return parsedRequest, fmt.Errorf("%s invalid collection %s %s",
			parsedRequest.RequestID, request.Host, err)
	}

	pathDir, pathKey := path.Split(request.URL.Path)
	if !validKeyRegexp.MatchString(pathKey) {
		return parsedRequest, fmt.Errorf("%s invalid key %s",
			parsedRequest.RequestID, request.URL.Path)
	}

	parsedRequest.Key = pathKey

	switch pathDir {
	case "/data/":
		if upperCaseMethod == "DELETE" {
			parsedRequest.Type = DeleteKey
			return parsedRequest, nil
		}

		if upperCaseMethod != "POST" {
			return parsedRequest, fmt.Errorf("%s invalid method %s for path %s",
				parsedRequest.RequestID, request.Method, request.URL.Path)
		}

		switch request.URL.Query().Get("action") {
		case "":
			parsedRequest.Type = ArchiveKey
			return parsedRequest, nil
		case "delete":
			parsedRequest.Type = DeleteKey
			return parsedRequest, nil
		}

	case "/conjoined/":
		if upperCaseMethod != "POST" {
			return parsedRequest, fmt.Errorf("%s invalid method %s for path %s",
				parsedRequest.RequestID, request.Method, request.URL.Path)
		}

		switch request.URL.Query().Get("action") {
		case "start":
			parsedRequest.Type = StartConjoined
			return parsedRequest, nil
		case "finish":
			parsedRequest.Type = FinishConjoined
			parsedRequest.UnifiedID, err = parseConjoinedIdentifier(
				request.URL.Query().Get("conjoined_identifier"))
			if err != nil {
				return parsedRequest, fmt.Errorf("%s conjoined identifier %s %s",
					parsedRequest.RequestID,
					request.URL.Query().Get("conjoined_identifier"), err)
			}
			return parsedRequest, nil
		case "abort":
			parsedRequest.Type = AbortConjoined
			parsedRequest.UnifiedID, err = parseConjoinedIdentifier(
				request.URL.Query().Get("conjoined_identifier"))
			if err != nil {
				return parsedRequest, fmt.Errorf("%s conjoined identifier %s %s",
					parsedRequest.RequestID,
					request.URL.Query().Get("conjoined_identifier"), err)
			}
			return parsedRequest, nil
		}

	default:
		return parsedRequest, fmt.Errorf("%s invalid path %s; (%s, %s)",
			parsedRequest.RequestID, request.URL.Path, pathDir, pathKey)
	}

	return parsedRequest, fmt.Errorf("%s unparsable request; method %s; path %s",
		parsedRequest.RequestID, request.Method, request.URL.Path)
}

func parseCollectionName(host string) (string, error) {
	index := strings.Index(host, ".")
	if index == -1 {
		return "", fmt.Errorf("invalid hostname lacks '.' %s", host)
	}
	collectionName := host[:index]
	if !validCollectionRegexp.MatchString(collectionName) {
		return "", fmt.Errorf("invalid collection name %q", collectionName)
	}

	return collectionName, nil
}

func parseConjoinedIdentifier(publicIdentifier string) (uint64, error) {
	return 0, fmt.Errorf("not implemented")
}
