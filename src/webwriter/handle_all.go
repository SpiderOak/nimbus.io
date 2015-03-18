package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"access"
	"auth"
	"centraldb"
	"types"
	"unifiedid"

	"webwriter/handler"
	"webwriter/req"
)

type handlerEntry struct {
	Func   handler.RequestHandler
	Access access.AccessType
}

type handlerStruct struct {
	CentralDB        centraldb.CentralDB
	DataWriterChans  []DataWriterClientChan
	UnifiedIDFactory unifiedid.UnifiedIDFactory
	Dispatch         map[req.RequestType]handlerEntry
}

var (
	clusterName      = os.Getenv("NIMBUSIO_CLUSTER_NAME")
	localNodeName    = os.Getenv("NIMBUSIO_NODE_NAME")
	forwardedForKey  = http.CanonicalHeaderKey("x-forwarded-for")
	refererKey       = http.CanonicalHeaderKey("referer")
	authorizationKey = http.CanonicalHeaderKey("authorization")
	timestampKey     = http.CanonicalHeaderKey("x-nimbus-io-timestamp")
)

// NewHandler returns an entity that implements the http.Handler interface
// this handles all incoming requests
func NewHandler(dataWriterChans []DataWriterClientChan) (http.Handler, error) {
	var err error

	h := handlerStruct{
		CentralDB:       centraldb.NewCentralDB(),
		DataWriterChans: dataWriterChans,
	}

	if h.UnifiedIDFactory, err = createUnifiedIDFactory(h.CentralDB); err != nil {
		return nil, err
	}

	h.Dispatch = map[req.RequestType]handlerEntry{
		req.RespondToPing: handlerEntry{Func: handler.RespondToPing,
			Access: access.NoAccess},
		req.ArchiveKey: handlerEntry{Func: handler.ArchiveKey,
			Access: access.Write},
		req.DeleteKey: handlerEntry{Func: handler.DeleteKey,
			Access: access.Delete},
		req.StartConjoined: handlerEntry{Func: handler.StartConjoined,
			Access: access.Write},
		req.FinishConjoined: handlerEntry{Func: handler.FinishConjoined,
			Access: access.Write},
		req.AbortConjoined: handlerEntry{Func: handler.AbortConjoined,
			Access: access.Write},
	}

	return &h, nil
}

func createUnifiedIDFactory(centralDB centraldb.CentralDB) (unifiedid.UnifiedIDFactory, error) {
	var err error
	var nodeIDMap map[string]uint32
	var localNodeID uint32
	var ok bool

	if nodeIDMap, err = centralDB.GetNodeIDsForCluster(clusterName); err != nil {
		return nil, err
	}

	if localNodeID, ok = nodeIDMap[localNodeName]; !ok {
		return nil, fmt.Errorf("unknown local node: '%s'", localNodeName)
	}

	return unifiedid.NewUnifiedIDFactory(localNodeID)
}

// ServeHTTP implements the http.Handler interface
// handles all HTTP requests

// https://<collection name>.nimbus.io/data/<key>
// https://<collection name>.nimbus.io/data/<key>?action=delete
// https://<collection name>.nimbus.io/conjoined/<key>?action=start
// https://<collection name>.nimbus.io/conjoined/<key>?action=finish&conjoined_identifier=<conjoined_identifier>
// https://<collection name>.nimbus.io/conjoined/<key>?action=abort&conjoined_identifier=<conjoined_identifier>

func (h *handlerStruct) ServeHTTP(responseWriter http.ResponseWriter,
	request *http.Request) {
	var err error
	var parsedRequest req.ParsedRequest
	var collectionRow types.CollectionRow
	var accessControl access.AccessControlType

	if parsedRequest, err = req.ParseRequest(request); err != nil {
		log.Printf("error: unparsable request: %s, method='%s'", err,
			request.Method)
		http.Error(responseWriter, "unparsable request", http.StatusBadRequest)
		return
	}

	if request.URL.Path != "/ping" {
		log.Printf("debug: %s method=%s, collection=%s path=%s query=%s %s",
			parsedRequest.RequestID, request.Method,
			parsedRequest.CollectionName, request.URL.Path,
			request.URL.RawQuery, request.RemoteAddr)
	}

	dispatchEntry, ok := h.Dispatch[parsedRequest.Type]
	if !ok {
		// this shouldn't happen
		log.Printf("error: unknown request type: %s", parsedRequest.Type)
		http.Error(responseWriter, "unknown request type",
			http.StatusInternalServerError)
		return
	}

	if dispatchEntry.Access == access.NoAccess {
		err = dispatchEntry.Func(responseWriter, request, parsedRequest,
			types.CollectionRow{})
		if err != nil {
			log.Printf("error: ping %s", err)
		}
		return
	}

	collectionRow, err = h.CentralDB.GetCollectionRow(
		parsedRequest.CollectionName)
	if err != nil {
		log.Printf("error: unknown collection: %s; %s",
			parsedRequest.CollectionName, err)
		http.Error(responseWriter, "unknown collection", http.StatusNotFound)
		return
	}

	accessControl, err = access.LoadAccessControl(collectionRow.AccessControl)
	if err != nil {
		log.Printf("error: unable to load access control: %s", err)
		http.Error(responseWriter, "unable to load access control",
			http.StatusInternalServerError)
		return
	}

	requesterIP, err := getRequesterIP(request.Header.Get(forwardedForKey))
	if err != nil {
		log.Printf("error: unable to get requester IP from headers: %s", err)
		http.Error(responseWriter, "unable to get requester IP",
			http.StatusBadRequest)
		return
	}

	referrer, err := getReferer(request.Header.Get(refererKey))
	if err != nil {
		log.Printf("error: unable to get referer: %s", err)
		http.Error(responseWriter, "unable to get referer",
			http.StatusBadRequest)
		return
	}

	accessStatus, err := access.CheckAccess(dispatchEntry.Access,
		accessControl, request.URL.Path, requesterIP, referrer)

	accessGranted := false
	switch accessStatus {
	case access.Allowed:
		accessGranted = true
	case access.RequiresPasswordAuthentication:
		err = auth.PasswordAuthentication(h.CentralDB,
			collectionRow.CustomerID,
			request.Method,
			request.Header.Get(timestampKey),
			request.URL.Path,
			request.Header.Get(authorizationKey))
		accessGranted = err == nil
	case access.Forbidden:
	default:
		log.Printf("error: unknown access: %s", accessStatus)
		http.Error(responseWriter, "unknown access",
			http.StatusInternalServerError)
		return
	}

	if !accessGranted {
		log.Printf("warning: access forbidden")
		http.Error(responseWriter, "invalid", http.StatusForbidden)
		return
	}

	err = dispatchEntry.Func(responseWriter, request, parsedRequest,
		collectionRow)
	if err != nil {
		log.Printf("error: %s handler failed: %s", parsedRequest.Type, err)
		http.Error(responseWriter, "handler failed",
			http.StatusInternalServerError)
		return
	}
}

func getRequesterIP(forwardwedForHeader string) (net.IP, error) {
	if forwardwedForHeader == "" {
		return nil, fmt.Errorf("no data for %s", forwardedForKey)
	}

	//  the header can have multiple forwards of the form
	// address1, address2, ...
	// we want the first one which should be the original sender's
	forwardSlice := strings.Split(forwardwedForHeader, ", ")
	addressAndPort := forwardSlice[0]
	address := strings.Split(addressAndPort, ":")

	ip := net.ParseIP(address[0])
	if ip == nil {
		return nil, fmt.Errorf("unable to parse address '%s'", addressAndPort)
	}

	return ip, nil
}

func getReferer(refererHeader string) (string, error) {
	// it's OK to not have a referer
	if len(refererHeader) == 0 {
		return "", nil
	}

	referrerURL, err := url.Parse(refererHeader)
	if err != nil {
		return "", err
	}

	return referrerURL.Path, nil
}
