package tools

import (
	"io"

	"fog"
)

type sizeReader struct {
	MaxSize     uint64
	CurrentSize uint64
	ReportedMB  uint64
}

// NewSizeReader returns an entity which implements the io.Reader interface
// it enables read of discardable data up to the specified size
func NewSizeReader(size uint64) io.Reader {
	return &sizeReader{MaxSize: size}
}

func (reader *sizeReader) Read(p []byte) (int, error) {
	if reader.CurrentSize >= reader.MaxSize {
		return 0, io.EOF
	}

	bytesLeft := reader.MaxSize - reader.CurrentSize
	var bytesToSend uint64
	var err error

	if bytesLeft > uint64(cap(p)) {
		bytesToSend = uint64(cap(p))
	} else {
		bytesToSend = bytesLeft
		err = io.EOF
	}

	for i := 0; i < int(bytesToSend); i++ {
		p[i] = 'a'
	}
	reader.CurrentSize += bytesToSend

	currentMB := reader.CurrentSize / (1024 * 1024)
	if currentMB > reader.ReportedMB {
		fog.Debug("size reader %dmb", currentMB)
		reader.ReportedMB = currentMB
	}

	return int(bytesToSend), err
}
