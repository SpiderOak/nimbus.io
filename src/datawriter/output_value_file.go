package main

import (
	"database/sql"
)

type OutputValueFile interface {
	Size() uint64
}

type outputValueFile struct {
	bytesWritten uint64
}

func NewOutputValueFile(sqlDB *sql.DB) (OutputValueFile, error) {
	var valueFile outputValueFile

	return &valueFile, nil
}

func (valueFile *outputValueFile) Size() uint64 {
	return valueFile.bytesWritten
}
