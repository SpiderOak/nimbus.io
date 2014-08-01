package passon

import (
	"bytes"
	"io"
	"io/ioutil"
)

// NewBodyReadCloser returns an entity that implements the io.ReadCloser
// interface. It wraps whatever bytes are leftover in the buffer from reading
// the http.Request, and the socket connection for reading the rest of the
// body
func NewBodyReadCloser(contentLength uint64, buffer []byte,
	conn io.ReadCloser) io.ReadCloser {

	return ioutil.NopCloser(
		io.LimitReader(
			io.MultiReader(bytes.NewReader(buffer), conn),
			int64(contentLength)))

}
