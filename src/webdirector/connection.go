package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"

	"fog"
	"tools"

	"webdirector/routing"
)

// handleConnection manages one HTTP connection
// expected to be run in a goroutine
func handleConnection(router routing.Router, conn net.Conn) {
	defer conn.Close()
	const bufferSize = 64 * 1024
	var err error

	requestID, err := tools.CreateUUID()
	if err != nil {
		fog.Error("%s tools.CreateUUID(): %s", conn.RemoteAddr().String(), err)
		return
	}
	fog.Info("%s starts %s", requestID, conn.RemoteAddr().String())

	request, err := http.ReadRequest(bufio.NewReaderSize(conn, bufferSize))
	if err != nil {
		fog.Error("%s %s ReadRequest failed: %s", requestID,
			conn.RemoteAddr().String(), err)
		fog.Info("%s aborts", requestID)
		return
	}

	// change the URL to point to our internal host
	request.URL.Host, err = router.Route(requestID, request)
	if err != nil {
		routerErr, ok := err.(routing.RouterError)
		if ok {
			fog.Error("%s %s, %s router error: %s",
				requestID, request.Method, request.URL, err)
			sendErrorReply(conn, routerErr.HTTPCode(), routerErr.ErrorMessage())
		} else {
			fog.Error("%s %s, %s Unexpected error type: %T %s",
				requestID, request.Method, request.URL, err, err)
		}
		fog.Info("%s aborts", requestID)
		return
	}
	request.URL.Scheme = "http"

	// heave the incoming RequestURI: can't be set in a client request
	request.RequestURI = ""

	modifyHeaders(request, conn.RemoteAddr().String(), requestID)
	fog.Debug("%s routing %s %s", requestID, request.Method, request.URL)

	// TODO: cache the connection to the internal server
	internalConn, err := net.Dial("tcp", request.URL.Host)
	if err != nil {
		fog.Error("%s %s, %s unable to dial internal server: %s",
			requestID, request.Method, request.URL, err)
		sendErrorReply(conn, http.StatusInternalServerError, err.Error())
		fog.Info("%s aborts", requestID)
		return
	}
	defer internalConn.Close()

	err = request.Write(bufio.NewWriterSize(internalConn, bufferSize))
	if err != nil {
		fog.Error("%s %s, %s request.Write: %s",
			requestID, request.Method, request.URL, err)
		sendErrorReply(conn, http.StatusInternalServerError, err.Error())
		fog.Info("%s aborts", requestID)
		return
	}
	request.Body.Close()

	response, err := http.ReadResponse(bufio.NewReaderSize(internalConn, bufferSize),
		request)
	if err != nil {
		fog.Error("%s %s, %s http.ReadResponse: %s",
			requestID, request.Method, request.URL, err)
		sendErrorReply(conn, http.StatusInternalServerError, err.Error())
		fog.Info("%s aborts", requestID)
		return
	}

	if err := response.Write(bufio.NewWriterSize(conn, bufferSize)); err != nil {
		fog.Error("%s %s, %s error sending response: %s",
			requestID, request.Method, request.URL, err)
	}
	response.Body.Close()

	fog.Info("%s ends (%d) %s", requestID, response.StatusCode, response.Status)
}

// sendErrorReply sends an error reply to the client
func sendErrorReply(conn net.Conn, httpCode int, errorMessage string) {
	reply := fmt.Sprintf("HTTP/1.0 %d %s\r\n\r\n%s",
		httpCode, http.StatusText(httpCode), errorMessage)
	if _, err := conn.Write([]byte(reply)); err != nil {
		fog.Error("Write error: %s %s", reply, err)
	}
}

// modifyHeaders tweaks the the headers for handoff to the internal server
func modifyHeaders(request *http.Request, remoteAddress, requestID string) {
	/*
		alan says this:

		still I don't feel great about this. from a security standpoint,
		signaling access control information (such as IP address, which some
		collections set access policy for) inside a stream of data controlled by
		the attacker is a bad idea.  downstream http parsers are probably robust
		enough to not be easily trickable, but I'd feel better about signaling
		out of band, or adding another header with a HMAC from a secret key.
	*/

	forwardedForKey := http.CanonicalHeaderKey("x-forwarded-for")
	existingForwardedFor := request.Header.Get(forwardedForKey)
	var newForwardedFor string
	if existingForwardedFor == "" {
		newForwardedFor = remoteAddress
	} else {
		newForwardedFor = fmt.Sprintf("%s, %s", existingForwardedFor,
			remoteAddress)
	}
	request.Header.Set(forwardedForKey, newForwardedFor)

	requestIDKey := http.CanonicalHeaderKey("x-nimbus-io-user-request-id")
	request.Header.Set(requestIDKey, requestID)
}
