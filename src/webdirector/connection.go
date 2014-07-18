package main

import (
	"bufio"
	"net"
	"net/http"

	"fog"
)

// handleConnection manages one HTTP connection
// expected to be run in a goroutine
func handleConnection(conn net.Conn) {
	defer conn.Close()

	request, err := http.ReadRequest(bufio.NewReaderSize(conn, 4096))
	if err != nil {
		fog.Error("%s http.ReadRequest failed: %s",
			conn.RemoteAddr().String(), err)
		return
	}

	fog.Debug("got request %s", request)
}
