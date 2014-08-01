package main

import (
	"io"

	"fog"
)

type sizeReader struct {
	MaxSize     uint64
	CurrentSize uint64
}

// NewSizeReader returns an entity which implements the io.Reader interface
// it enables read of unimportant data up to the specified size
func NewSizeReader(size uint64) io.Reader {
	return &sizeReader{MaxSize: size}
}

func (reader *sizeReader) Read(p []byte) (int, error) {
	if reader.CurrentSize >= reader.MaxSize {
		return 0, io.EOF
	}

	fog.Debug("SizeReader: requested=%d, current=%d, max=%d",
		cap(p), reader.CurrentSize, reader.MaxSize)

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

	fog.Debug("SizeReader: return %d, %s", bytesToSend, err)
	return int(bytesToSend), err
}
