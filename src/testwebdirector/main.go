package main

import (
	"crypto/tls"
	"io"
	"net/http"
	"os"

	"fog"
)

// main entry point
func main() {
	fog.Info("testwebdirector starts")

	go server(os.Getenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST"))

	config := tls.Config{InsecureSkipVerify: true}
	transport := &http.Transport{TLSClientConfig: &config}
	client := http.Client{Transport: transport}

	if _, err := client.Get("https://127.0.0.1:12345"); err != nil {
		fog.Error("get %s", err)
	}
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
