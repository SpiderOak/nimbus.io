package routing

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"centraldb"

	"webdirector/avail"
	"webdirector/mgmtapi"
)

type routerTestEntry struct {
	testName         string
	method           string
	uri              string
	body             io.Reader
	headers          map[string][]string
	hosts            map[string][]string
	availableHosts   []string
	expectedHostPort string
	expectedError    RouterError
}

const (
	mgmtApiHost      = "mgmtApiHost"
	validCollection  = "aaa"
	canonicalHostKey = "Host"
	requestID        = "xxxxxxxxxxxxxxxx" // this should be a UUID
)

var (
	routerTestData     []routerTestEntry
	validDomain        string
	validHosts         []string
	hostsForCollection map[string][]string
)

func init() {
	var headerMap map[string][]string

	_ = os.Setenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST", mgmtApiHost)

	validDomain = fmt.Sprintf("%s.%s", validCollection, serviceDomain)
	validHosts = []string{"host01", "host02", "host03", "host04",
		"host05", "host06", "host07", "host08", "host09", "host10"}
	hostsForCollection = map[string][]string{validCollection: validHosts}

	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "empty request",
			method:           "",
			uri:              "",
			body:             nil,
			headers:          nil,
			expectedHostPort: "",
			expectedError:    routerErrorImpl{httpCode: http.StatusBadRequest}})

	headerMap = map[string][]string{canonicalHostKey: []string{"xxx"}}
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "invalid host",
			method:           "",
			uri:              "",
			body:             nil,
			headers:          headerMap,
			expectedHostPort: "",
			expectedError:    routerErrorImpl{httpCode: http.StatusNotFound}})

	headerMap = map[string][]string{canonicalHostKey: []string{serviceDomain}}
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "serviceDomain host",
			method:           "",
			uri:              "",
			body:             nil,
			headers:          headerMap,
			expectedHostPort: mgmtApiHost,
			expectedError:    nil})

	headerMap = map[string][]string{canonicalHostKey: []string{validDomain}}
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "no hosts for collection",
			method:           "GET",
			uri:              "",
			body:             nil,
			headers:          headerMap,
			expectedHostPort: "",
			expectedError:    routerErrorImpl{httpCode: http.StatusNotFound}})

	headerMap = map[string][]string{canonicalHostKey: []string{validDomain}}
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "no hosts available for collection",
			method:           "GET",
			uri:              "",
			body:             nil,
			hosts:            hostsForCollection,
			headers:          headerMap,
			expectedHostPort: "",
			expectedError:    routerErrorImpl{httpCode: http.StatusServiceUnavailable}})

	headerMap = map[string][]string{canonicalHostKey: []string{validDomain}}
	writerPort := os.Getenv("NIMBUSIO_WEB_WRITER_PORT")
	availHostPort := fmt.Sprintf("%s:%s", "host07", writerPort)
	routerTestData = append(routerTestData,
		routerTestEntry{
			testName:         "round robin host",
			method:           "POST",
			uri:              "",
			body:             nil,
			hosts:            hostsForCollection,
			availableHosts:   []string{availHostPort},
			headers:          headerMap,
			expectedHostPort: availHostPort,
			expectedError:    nil})
}

func TestRouter(t *testing.T) {
	managmentAPIDests, err := mgmtapi.NewManagementAPIDestinations()
	if err != nil {
		t.Fatalf("NewManagementAPIDestinations: %s", err)
	}

	for n, testEntry := range routerTestData {
		centralDB := newMockCentralDB(testEntry.hosts)
		availableHosts := avail.NewMockAvailability(testEntry.availableHosts)

		router := NewRouter(managmentAPIDests, centralDB, availableHosts)

		req, err := http.NewRequest(testEntry.method, testEntry.uri,
			testEntry.body)
		if err != nil {
			t.Fatalf("http.NewRequest %s", err)
		}

		for key := range testEntry.headers {
			req.Header[key] = testEntry.headers[key]
		}
		req.Host = req.Header.Get(canonicalHostKey)

		hostPort, err := router.Route(requestID, req)

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
				t.Fatalf("Route %d %s Unexpected HTTP Status: %s expecting %s",
					n, testEntry.testName, routerErr, testEntry.expectedError)
			}
		} else if err != nil {
			t.Fatalf("Route %d %s, unexpected error %s", n,
				testEntry.testName, err)

		} else if hostPort != testEntry.expectedHostPort {
			t.Fatalf("Route %d %s Unexpected host:port: %s expecting %s",
				n, testEntry.testName, hostPort, testEntry.expectedHostPort)
		}
	}
}

type mockCentralDB struct {
	hostsForCollection map[string][]string
}

func newMockCentralDB(hostsForCollection map[string][]string) centraldb.CentralDB {
	return mockCentralDB{hostsForCollection: hostsForCollection}
}

// Close releases the resources held by the CentralDB
func (m mockCentralDB) Close() {}

// GetHostsForCollection returns a slice of the host names that hold data
// for the collection
func (m mockCentralDB) GetHostsForCollection(collectionName string) ([]string, error) {
	hosts, _ := m.hostsForCollection[collectionName]
	return hosts, nil
}

// GetNodeIDsForCluster returns a map of node id keyed by node name,
// based on the cluster name
func (m mockCentralDB) GetNodeIDsForCluster(clusterName string) (map[string]uint32, error) {
	return nil, fmt.Errorf("GetNodeIDsForCluster not implemented")
}
