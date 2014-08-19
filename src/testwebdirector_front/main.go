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
	fog.Info("front starts")

	serviceDomain := os.Getenv("NIMBUS_IO_SERVICE_DOMAIN")

	config := tls.Config{InsecureSkipVerify: true}
	transport := &http.Transport{TLSClientConfig: &config}
	client := http.Client{Transport: transport}

	var contentLength uint64 = 1024 * 1024 * 1024
	fog.Debug("start post %dmb", contentLength/(1024*1024))

	url := fmt.Sprintf("https://%s/admin", serviceDomain)
	bodyReader := tools.NewSizeReader(contentLength)
	request, err := http.NewRequest("POST", url, bodyReader)
	if err != nil {
		fog.Critical("NewRequest failed %s")
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

	fog.Debug("finished post %s", response.Status)

	fog.Info("front ends")
}
