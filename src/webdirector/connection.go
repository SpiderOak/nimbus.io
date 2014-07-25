package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"

	"fog"

	"webdirector/routing"
)

// handleConnection manages one HTTP connection
// expected to be run in a goroutine
func handleConnection(router routing.Router, conn net.Conn) {
	defer conn.Close()
	var err error

	reader := bufio.NewReader(conn)
	request, err := http.ReadRequest(reader)
	if err != nil {
		fog.Error("%s http.ReadRequest failed: %s",
			conn.RemoteAddr().String(), err)
		return
	}

	fog.Debug("got request %s", request)

	/*hostPort*/ _, err = router.Route(request)
	if err != nil {
		routerErr, ok := err.(routing.RouterError)
		if ok {
			fog.Error("%s, %s router error: %s",
				request.Method, request.URL, err)
			sendErrorReply(conn, routerErr.HTTPCode(), routerErr.ErrorMessage())
		} else {
			fog.Error("%s, %s Unexpected error type: %T %s",
				request.Method, request.URL, err, err)
		}
		return
	}

	// routing OK, now proxy
	sendErrorReply(conn, http.StatusNotImplemented, "handleConnection")
}

// sendErrorReply sends an error reply to the client
func sendErrorReply(conn net.Conn, httpCode int, errorMessage string) {
	reply := fmt.Sprintf("HTTP/1.0 %d %s\r\n\r\n%s",
		httpCode, http.StatusText(httpCode), errorMessage)
	if _, err := conn.Write([]byte(reply)); err != nil {
		fog.Error("Write error: %s %s", reply, err)
	}
}
