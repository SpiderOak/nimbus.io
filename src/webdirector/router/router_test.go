package router

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"webdirector/hosts"
	"webdirector/mgmtapi"
)

type routerTestEntry struct {
	testName         string
	method           string
	uri              string
	body             io.Reader
	headers          map[string][]string
	hosts            map[string][]string
	expectedHostPort string
	expectedError    RouterError
}

const (
	mgmtApiHost     = "mgmtApiHost"
	validCollection = "aaa"
)

var (
	routerTestData      []routerTestEntry
	validDomain         string
	hostsForValidDomain []string
)

func init() {
	var headerMap map[string][]string

	_ = os.Setenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST", mgmtApiHost)

	validDomain = fmt.Sprintf("%s.%s", validCollection, serviceDomain)
	hostsForValidDomain = []string{"host01", "host02", "host03", "host04",
		"host05", "host06", "host07", "host08", "host09", "host10"}

	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "empty request",
			method:           "",
			uri:              "",
			body:             nil,
			headers:          nil,
			expectedHostPort: "",
			expectedError:    routerErrorImpl{httpCode: http.StatusBadRequest}})

	headerMap = map[string][]string{"HOST": []string{"xxx"}}
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "invalid host",
			method:           "",
			uri:              "",
			body:             nil,
			headers:          headerMap,
			expectedHostPort: "",
			expectedError:    routerErrorImpl{httpCode: http.StatusNotFound}})

	headerMap = map[string][]string{"HOST": []string{serviceDomain}}
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "serviceDomain host",
			method:           "",
			uri:              "",
			body:             nil,
			headers:          headerMap,
			expectedHostPort: mgmtApiHost,
			expectedError:    nil})

	headerMap = map[string][]string{"HOST": []string{validDomain}}
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "no hosts for collection",
			method:           "GET",
			uri:              "",
			body:             nil,
			headers:          headerMap,
			expectedHostPort: mgmtApiHost,
			expectedError:    nil})
}

func TestRouter(t *testing.T) {
	managmentAPIDests, err := mgmtapi.NewManagementAPIDestinations()
	if err != nil {
		t.Fatalf("NewManagementAPIDestinations: %s", err)
	}

	for n, testEntry := range routerTestData {
		hostsForCollection := hosts.NewMockHostsForCollection(testEntry.hosts)

		router := NewRouter(managmentAPIDests, hostsForCollection)

		req, err := http.NewRequest(testEntry.method, testEntry.uri,
			testEntry.body)
		if err != nil {
			t.Fatalf("http.NewRequest %s", err)
		}

		for key := range testEntry.headers {
			req.Header[key] = testEntry.headers[key]
		}

		hostPort, err := router.Route(req)

		if testEntry.expectedError != nil {
			if err == nil {
				t.Fatalf("Route %d %s, expecting error %s", n,
					testEntry.testName, testEntry.expectedError)
			}
			routerErr, ok := err.(RouterError)
			if !ok {
				t.Fatalf("Route %d %s Unexpected error type: %T %s",
					n, testEntry.testName, err, err)
			}
			if routerErr.HTTPCode() != testEntry.expectedError.HTTPCode() {
				t.Fatalf("Route %d %s Unexpected HTTP Status: %d expecting %d",
					n, testEntry.testName, routerErr.HTTPCode(),
					testEntry.expectedError.HTTPCode())
			}
		} else {
			if err != nil {
				t.Fatalf("Route %d %s, unexpected error %s", n,
					testEntry.testName, err)
			}
		}

		if hostPort != testEntry.expectedHostPort {
			t.Fatalf("Route %d %s Unexpected host:port: %s expecting %s",
				n, testEntry.testName, hostPort, testEntry.expectedHostPort)
		}
	}
}
