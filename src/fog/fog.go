/*Package fog provides log with Python (actually log4j) style named levels
**
**Dr Fogout's handy logger
**
 */
package fog

import (
	"fmt"
	"log"
	"runtime"
)

const (
	callDepth      = 2
	DebugPrefix    = "DEBUG:   "
	InfoPrefix     = "INFO:    "
	WarnPrefix     = "WARNING: "
	ErrorPrefix    = "ERROR:   "
	CriticalPrefix = "CRITICAL:"
)

func init() {
	log.SetFlags(log.LstdFlags)
}

// Set the log flags
func SetFlags(flags int) {
	log.SetFlags(flags)
}

// Debug prepends literal 'DEBUG' to log message
func Debug(text string, args ...interface{}) {
	log.Printf("%s %s", DebugPrefix, fmt.Sprintf(text, args...))
}

// Info prepends literal 'INFO' to log message
func Info(text string, args ...interface{}) {
	log.Printf("%s %s", InfoPrefix, fmt.Sprintf(text, args...))
}

// Warn prepends literal 'WARNING' to log message
func Warn(text string, args ...interface{}) {
	log.Printf("%s %s", WarnPrefix, fmt.Sprintf(text, args...))
}

// Error prepends literal 'ERROR' to log message
// program continues to execute
func Error(text string, args ...interface{}) {
	log.Printf("%s %s", ErrorPrefix, fmt.Sprintf(text, args...))
}

// Critical prepends literal 'CRITICAL' to log message
// program terminates
func Critical(text string, args ...interface{}) {
	LogLocation()
	log.Fatalf("%s %s", CriticalPrefix, fmt.Sprintf(text, args...))
}

func LogLocation() {
	_, file, line, ok := runtime.Caller(callDepth)
	if ok {
		log.Printf("RUNTIME: line #%d %q", line, file)
	}
}
