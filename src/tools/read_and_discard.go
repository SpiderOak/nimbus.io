package tools

import (
	"io"

	"fog"
)

func ReadAndDiscard(reader io.Reader) {
	var bytesRead uint64
	var mbReported uint64
	bufferSize := 64 * 1024
	buffer := make([]byte, bufferSize)

	for true {
		n, err := reader.Read(buffer)
		bytesRead += uint64(n)
		if err != nil {
			fog.Error("ReadAndDiscard error after %d bytes read: %s",
				bytesRead, err)
			break
		}
		mbRead := bytesRead / (1024 * 1024)
		if mbRead > mbReported {
			fog.Debug("ReadAndDiscard read %dmb", mbRead)
			mbReported = mbRead
		}
	}
}
