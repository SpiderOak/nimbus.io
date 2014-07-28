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

	fog.Debug("SizeReader %d requested %d of %d read so far",
		len(p), reader.CurrentSize, reader.MaxSize)

	bytesLeft := reader.MaxSize - reader.CurrentSize
	var bytesToSend uint64
	var err error

	if bytesLeft > uint64(len(p)) {
		bytesToSend = uint64(len(p))
	} else {
		bytesToSend = bytesLeft
		err = io.EOF
	}

	for i := 0; i < int(bytesToSend); i++ {
		p[i] = 'a'
	}
	reader.CurrentSize += bytesToSend

	return int(bytesToSend), err
}
