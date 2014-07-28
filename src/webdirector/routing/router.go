package routing

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"

	"fog"

	"webdirector/avail"
	"webdirector/hosts"
	"webdirector/mgmtapi"
)

type routerErrorImpl struct {
	httpCode     int
	errorMessage string
}

type routerImpl struct {
	managmentAPIDests  mgmtapi.ManagementAPIDestinations
	hostsForCollection hosts.HostsForCollection
	availability       avail.Availability
	requestCounter     uint64
	roundRobinCounter  uint64
}

var (
	serviceDomain          string
	destPortMap            map[string]string
	alwaysRouteToFirstNode bool
)

func init() {
	serviceDomain = os.Getenv("NIMBUS_IO_SERVICE_DOMAIN")

	readDestPort := os.Getenv("NIMBUSIO_WEB_PUBLIC_READER_PORT")
	writeDestPort := os.Getenv("NIMBUSIO_WEB_WRITER_PORT")
	destPortMap = map[string]string{
		"POST":   writeDestPort,
		"DELETE": writeDestPort,
		"PUT":    writeDestPort,
		"PATCH":  writeDestPort,
		"HEAD":   readDestPort,
		"GET":    readDestPort}

	alwaysRouteToFirstNode =
		os.Getenv("NIMBUSIO_WEB_DIRECTOR_ALWAYS_FIRST_NODE") == "1"
}

// NewRouter returns an entity that implements the Router interface
func NewRouter(managmentAPIDests mgmtapi.ManagementAPIDestinations,
	hostsForCollection hosts.HostsForCollection,
	availability avail.Availability) Router {

	return &routerImpl{managmentAPIDests: managmentAPIDests,
		hostsForCollection: hostsForCollection, availability: availability}
}

// Route reads a request and decides where it should go <host:port>
func (router *routerImpl) Route(requestID string, req *http.Request) (string, error) {
	var err error

	hostName := req.Host

	router.requestCounter += 1
	fog.Debug("%s host=%s, method=%s, URL=%s", requestID, hostName, req.Method,
		req.URL)

	// TODO: be able to handle http requests from http 1.0 clients w/o a
	// host header to at least the website, if nothing else.

	if hostName == "" {
		return "", routerErrorImpl{httpCode: http.StatusBadRequest,
			errorMessage: "HOST header not found"}
	}
	routingHostName := strings.Split(hostName, ":")[0]
	if !strings.HasSuffix(routingHostName, serviceDomain) {
		return "", routerErrorImpl{httpCode: http.StatusNotFound,
			errorMessage: fmt.Sprintf("Invalid HOST '%s'", routingHostName)}
	}

	var routingMethod string
	var routedHost string

	if routingHostName == serviceDomain {
		// this is not a request specific to any particular collection
		// TODO: figure out how to route these requests.
		// in production, this might not matter.
		routingMethod = "management API"
		routedHost = router.managmentAPIDests.Next()
		fog.Debug("%s %s routed to %s by %s", requestID, req.URL.Path,
			routedHost, routingMethod)
		return routedHost, nil
	}

	destPort, ok := destPortMap[req.Method]
	if !ok {
		return "", routerErrorImpl{httpCode: http.StatusBadRequest,
			errorMessage: fmt.Sprintf("Unknown method '%s'", req.Method)}
	}

	collectionName := parseCollectionFromHostName(routingHostName)
	if collectionName == "" {
		return "", routerErrorImpl{httpCode: http.StatusNotFound,
			errorMessage: fmt.Sprintf("Unparseable host name '%s'", hostName)}
	}

	hostsForCollection, err := router.hostsForCollection.GetHostNames(collectionName)
	if err != nil {
		return "", routerErrorImpl{httpCode: http.StatusNotFound,
			errorMessage: fmt.Sprintf("no hosts for collection '%s'", collectionName)}
	}

	availableHosts, err := router.availability.AvailableHosts(
		hostsForCollection, destPort)
	if err != nil {
		return "", routerErrorImpl{httpCode: http.StatusInternalServerError,
			errorMessage: fmt.Sprintf("collection '%s': %s", collectionName, err)}
	}
	if len(availableHosts) == 0 {
		// XXX: the python web_director retries here, after a delay.
		// IMO, that's what HTTP Status 503 is for
		return "", routerErrorImpl{httpCode: http.StatusServiceUnavailable,
			errorMessage: fmt.Sprintf("no hosts available for collection '%s'",
				collectionName)}
	}

	switch {
	case alwaysRouteToFirstNode:
		routingMethod = "NIMBUSIO_WEB_DIRECTOR_ALWAYS_FIRST_NODE"
		routedHost = availableHosts[0]
	case (req.Method == "GET" || req.Method == "HEAD") &&
		strings.HasPrefix(req.URL.Path, "/data/") &&
		len(req.URL.Path) > len("/data/"):
		routedHost, err = consistentHashDest(hostsForCollection, availableHosts,
			collectionName, req.URL.Path)
		if err != nil {
			return "", routerErrorImpl{httpCode: http.StatusInternalServerError,
				errorMessage: fmt.Sprintf("collection '%s': %s", collectionName, err)}
		}
		routingMethod = "hash"
	default:
		if router.roundRobinCounter == 0 {
			// start the round robin dispatcher at a random number, so all the
			// workers don't start on the same point.
			n, err := rand.Int(rand.Reader, big.NewInt(int64(len(hostsForCollection))))
			if err != nil {
				return "", routerErrorImpl{httpCode: http.StatusInternalServerError,
					errorMessage: fmt.Sprintf("collection '%s': %s", collectionName, err)}
			}
			router.roundRobinCounter = n.Uint64()
		} else {
			router.roundRobinCounter += 1
		}

		// XXX: the python version works with hostsForCollection and then tries
		// to find one in availableHosts. IMO, assuming the group of
		// available hosts is fairly stable, we get the same result working
		// strictly with availableHosts
		i := int(router.roundRobinCounter % uint64(len(availableHosts)))
		routedHost = availableHosts[i]
		routingMethod = "round robin"
	}

	fog.Debug("%s %s %s routed to %s by %s", requestID, collectionName,
		req.URL.Path, routedHost, routingMethod)

	return fmt.Sprintf("%s:%s", routedHost, destPort), nil
}

func (err routerErrorImpl) Error() string {
	return fmt.Sprintf("Router Error (%d) %s", err.httpCode, err.errorMessage)
}

func (err routerErrorImpl) HTTPCode() int {
	return err.httpCode
}

func (err routerErrorImpl) ErrorMessage() string {
	return err.errorMessage
}
