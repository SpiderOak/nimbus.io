package tools

import (
	"io"
	"io/ioutil"

	"fog"
)

func ReadAndDiscard(reader io.Reader) {
	fog.Debug("ReadAndDiscard starts")
	n, err := io.Copy(ioutil.Discard, reader)
	fog.Debug("ReadAndDiscard: %d, %s", n, err)
}
