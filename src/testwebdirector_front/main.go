package main

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"

	"fog"
	"tools"
)

// main entry point
func main() {
	serviceDomain := os.Getenv("NIMBUS_IO_SERVICE_DOMAIN")

	useTLS := os.Getenv("NIMBUS_IO_SERVICE_SSL") == "1"
	fog.Info("front starts; service domain = %s, TLS = %t", serviceDomain, useTLS)

	//	testGET(serviceDomain, useTLS)
	testPOST(serviceDomain, useTLS)

	fog.Info("front ends")
}

func getClient(useTLS bool) (*http.Client, string) {
	var client *http.Client
	var scheme string

	if useTLS {
		config := tls.Config{InsecureSkipVerify: true}
		transport := http.Transport{TLSClientConfig: &config}
		client = &http.Client{Transport: &transport}
		scheme = "https"
	} else {
		client = http.DefaultClient
		scheme = "http"
	}

	return client, scheme
}

func testGET(serviceDomain string, useTLS bool) {

	fog.Debug("start get")

	client, scheme := getClient(useTLS)

	url := fmt.Sprintf("%s://%s/admin", scheme, serviceDomain)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fog.Critical("NewRequest (GET) failed %s", err)
	}
	response, err := client.Do(request)
	if err != nil {
		fog.Error("get %s", err)
		return
	}

	fog.Debug("reading GET response body")
	tools.ReadAndDiscard(response.Body)
	response.Body.Close()

	fog.Debug("finished get %s", response.Status)
}

func testPOST(serviceDomain string, useTLS bool) {
	var contentLength uint64 = 1024 * 1024 * 1024
	fog.Debug("start post %dmb", contentLength/(1024*1024))

	client, scheme := getClient(useTLS)

	url := fmt.Sprintf("%s://%s/admin", scheme, serviceDomain)
	bodyReader := tools.NewSizeReader(contentLength)
	request, err := http.NewRequest("POST", url, bodyReader)
	if err != nil {
		fog.Critical("NewRequest (POST) failed %s", err)
	}
	request.TransferEncoding = []string{"identity"}
	request.Header.Add("Content-Type", "text/plain")
	request.Header.Add("Content-Length", fmt.Sprintf("%s", contentLength))
	request.ContentLength = int64(contentLength)
	response, err := client.Do(request)
	if err != nil {
		fog.Error("post %s", err)
		return
	}
	tools.ReadAndDiscard(response.Body)
	response.Body.Close()

	fog.Debug("finished post %s", response.Status)
}
