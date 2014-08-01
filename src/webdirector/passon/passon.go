package passon

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	requestReader := bytes.NewReader(buffer[:requestSize])
	request, err := http.ReadRequest(bufio.NewReader(requestReader))
	if err != nil {
		return nil, fmt.Errorf("http.ReadRequest failed %s", err)
	}
	contentLengthStr := request.Header.Get("Content-Length")

	if contentLengthStr != "" {
		contentLength, err := strconv.ParseUint(contentLengthStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid content-length '%s', err",
				contentLengthStr, err)
		}
		request.Body = NewBodyReadCloser(contentLength,
			buffer[requestSize:bytesRead], reader)
	}

	return request, nil
}
