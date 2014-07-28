package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"

	"fog"
)

// main entry point
func main() {
	fog.Info("testwebdirector starts")

	serviceDomain := os.Getenv("NIMBUS_IO_SERVICE_DOMAIN")

	go server(os.Getenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST"))

	config := tls.Config{InsecureSkipVerify: true}
	transport := &http.Transport{TLSClientConfig: &config}
	client := http.Client{Transport: transport}

	fog.Debug("get")
	url := fmt.Sprintf("https://%s/satan", serviceDomain)
	response, err := client.Get(url)
	if err != nil {
		fog.Error("get %s", err)
	}
	fog.Debug("get response %s", response)

	fog.Debug("post")
	url = fmt.Sprintf("https://%s.%s/satan", "collection01", serviceDomain)
	bodyReader := NewSizeReader(1024)
	response, err = client.Post(url, "text/plain", bodyReader)
	if err != nil {
		fog.Error("post %s", err)
	}
	fog.Debug("post response %s", response)

	fog.Info("testwebdirector ends")
}

func server(address string) {
	fog.Info("server listening to %s", address)
	http.HandleFunc("/", handleAll)
	http.ListenAndServe(address, nil)
}

func handleAll(w http.ResponseWriter, req *http.Request) {
	fog.Debug("got request %s", req)
	io.WriteString(w, "hello, world!\n")
}
