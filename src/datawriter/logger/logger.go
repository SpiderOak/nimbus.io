package logger

import (
	"fmt"

	"fog"
)

type Logger interface {

	// Debug prepends literal 'DEBUG' to log message
	Debug(text string, args ...interface{})

	// Info prepends literal 'INFO' to log message
	Info(text string, args ...interface{})

	// Warn prepends literal 'WARNING' to log message
	Warn(text string, args ...interface{})

	// Error prepends literal 'ERROR' to log message
	// program continues to execute
	Error(text string, args ...interface{})

	// Critical prepends literal 'CRITICAL' to log message
	// program terminates
	Critical(text string, args ...interface{})
}

type logData struct {
	UserRequestID string
	UnifiedID     uint64
	ConjoinedPart uint32
	SegmentNum    uint8
	Key           string
}

func NewLogger(userRequestID string, unifiedID uint64, conjoinedPart uint32,
	segmentNum uint8, key string) Logger {
	return logData{
		UserRequestID: userRequestID,
		UnifiedID:     unifiedID,
		ConjoinedPart: conjoinedPart,
		SegmentNum:    segmentNum,
		Key:           key}
}

// Debug prepends literal 'DEBUG' to log message
func (l logData) Debug(text string, args ...interface{}) {
	fog.Debug(l.prefix()+text, args...)
}

// Info prepends literal 'INFO' to log message
func (l logData) Info(text string, args ...interface{}) {
	fog.Info(l.prefix()+text, args...)
}

// Warn prepends literal 'WARNING' to log message
func (l logData) Warn(text string, args ...interface{}) {
	fog.Warn(l.prefix()+text, args...)
}

// Error prepends literal 'ERROR' to log message
// program continues to execute
func (l logData) Error(text string, args ...interface{}) {
	fog.Error(l.prefix()+text, args...)
}

// Critical prepends literal 'CRITICAL' to log message
// program terminates
func (l logData) Critical(text string, args ...interface{}) {
	fog.Critical(l.prefix()+text, args...)
}

func (l logData) prefix() string {
	return fmt.Sprintf("%s (%d %d %d) %s ", l.UserRequestID, l.UnifiedID,
		l.ConjoinedPart, l.SegmentNum, l.Key)
}
