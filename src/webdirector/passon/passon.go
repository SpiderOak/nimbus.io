package passon

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"

	"fog"
)

const (
	readSize = 4096
)

// ReadPassOnRequest reads an http.Request in a way that makes it available
// to be 'passed on' to an internal webserver. Specifically we try to avoid
// reading the entire body.
func ReadPassOnRequest(reader io.ReadCloser) (*http.Request, error) {
	// read a lump from the head of the stream, trying to get the full
	// Request
	buffer := make([]byte, readSize)
	bytesRead, err := reader.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("unable to read %d bytes %s", readSize, err)
	}

	// the request should be terminated by a blank line
	blankLine := []byte{'\r', '\n', '\r', '\n'}
	index := bytes.Index(buffer, blankLine)
	if index < 0 {
		return nil, fmt.Errorf("unable to find end of request")
	}

	requestSize := index + len(blankLine)
	fog.Debug("bytesRead = %d, requestSize = %d, buffer[:requestSize] = %q",
		bytesRead, requestSize, string(buffer[:requestSize]))

	requestReader := bytes.NewReader(buffer[:requestSize])
	request, err := http.ReadRequest(bufio.NewReader(requestReader))
	if err != nil {
		return nil, fmt.Errorf("http.ReadRequest failed %s", err)
	}

	if bytesRead > requestSize {
		request.Body = NewBodyReadCloser(buffer[requestSize:bytesRead], reader)
	}

	return request, nil
}
