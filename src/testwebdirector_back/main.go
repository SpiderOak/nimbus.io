package main

import (
	"os"

	"fog"
)

// main entry point
func main() {
	fog.Info("back starts")

	adminAddress := os.Getenv("NIMBUSIO_MANAGEMENT_API_REQUEST_DEST")
	server := NewServer("admin", adminAddress)
	server.Serve()

	fog.Info("back ends")
}
