package router

import (
	"io"
	"net/http"
	"testing"
)

type routerTestEntry struct {
	testName         string
	method           string
	uri              string
	body             io.Reader
	headers          map[string][]string
	expectedHostPort string
	expectedError    RouterError
}

var (
	routerTestData []routerTestEntry
)

func init() {
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "empty request",
			method:           "",
			uri:              "",
			body:             nil,
			headers:          nil,
			expectedHostPort: "",
			expectedError:    routerErrorImpl{httpCode: http.StatusBadRequest}})

	headerMap := map[string][]string{"HOST": []string{"xxx"}}
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "invalid host",
			method:           "",
			uri:              "",
			body:             nil,
			headers:          headerMap,
			expectedHostPort: "",
			expectedError:    routerErrorImpl{httpCode: http.StatusNotFound}})
}

func TestRouter(t *testing.T) {

	for n, testEntry := range routerTestData {
		router := NewRouter()

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
		}

		if hostPort != testEntry.expectedHostPort {
			t.Fatalf("Route %d %s Unexpected host:port: %s expecting %s",
				n, testEntry.testName, hostPort, testEntry.expectedHostPort)
		}
	}
}
