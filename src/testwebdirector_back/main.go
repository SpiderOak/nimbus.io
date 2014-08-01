package main

import (
	"fmt"
	"os"

	"fog"
)

// main entry point
func main() {
	fog.Info("testwebdirector_back starts")

	adminAddress := os.Getenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST")
	server := NewServer("admin", adminAddress)
	go server.Serve()

	readerAddress := fmt.Sprintf("127.0.0.1:%s", os.Getenv("NIMBUSIO_WEB_PUBLIC_READER_PORT"))
	server = NewServer("reader", readerAddress)
	go server.Serve()

	writerAddress := fmt.Sprintf("127.0.0.1:%s", os.Getenv("NIMBUSIO_WEB_WRITER_PORT"))
	server = NewServer("writer", writerAddress)
	server.Serve()

	fog.Info("testwebdirector_back ends")
}
