package passon

import (
	"bytes"
	"io"
)

type bodyReadCloser struct {
	Reader io.Reader
	Closer io.Closer
}

// NewBodyReadCloser returns an entity that implements the io.ReadCloser
// interface. It wraps whatever bytes are leftover in the buffer from reading
// the http.Request, and the socket connection for reading the rest of the
// body
func NewBodyReadCloser(buffer []byte, conn io.ReadCloser) io.ReadCloser {
	var rc bodyReadCloser
	rc.Reader = io.MultiReader(bytes.NewReader(buffer), conn)
	rc.Closer = conn

	return rc
}

// Read implements the io.Reader interface
func (b bodyReadCloser) Read(p []byte) (int, error) {
	return b.Reader.Read(p)
}

// Close implements the io.Closer interface
func (b bodyReadCloser) Close() error {
	return b.Closer.Close()
}
