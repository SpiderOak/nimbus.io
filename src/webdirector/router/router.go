package router

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"webdirector/mgmtapi"
)

type routerErrorImpl struct {
	httpCode     int
	errorMessage string
}

type routerImpl struct {
	managmentAPIDests mgmtapi.ManagementAPIDestinations
}

var (
	serviceDomain string
)

func init() {
	serviceDomain = os.Getenv("NIMBUS_IO_SERVICE_DOMAIN")
}

// NewRouter returns an entity that implements the Router interface
func NewRouter(managmentAPIDests mgmtapi.ManagementAPIDestinations) Router {
	return &routerImpl{managmentAPIDests: managmentAPIDests}
}

// Route reads a request and decides where it should go <host:port>
func (router *routerImpl) Route(req *http.Request) (string, error) {

	// TODO: be able to handle http requests from http 1.0 clients w/o a
	// host header to at least the website, if nothing else.
	hostName, ok := req.Header["HOST"]
	if !ok {
		return "", routerErrorImpl{httpCode: http.StatusBadRequest,
			errorMessage: "HOST header not found"}
	}
	routingHostName := strings.Split(hostName[0], ":")[0]
	if !strings.HasSuffix(routingHostName, serviceDomain) {
		return "", routerErrorImpl{httpCode: http.StatusNotFound,
			errorMessage: fmt.Sprintf("Invalid HOST '%s'", routingHostName)}
	}

	if routingHostName == serviceDomain {
		// this is not a request specific to any particular collection
		// TODO: figure out how to route these requests.
		// in production, this might not matter.
		return router.managmentAPIDests.Next(), nil
	}

	return "", nil
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
