package msg

import (
	"fmt"
	"regexp"
)

var (
	messageTypeRegexp *regexp.Regexp
)

func init() {
	// "message-type": "start-conjoined-archive"
	messageTypeRegexp = regexp.MustCompile(`"message-type"\s*\:\s*"(.+?)"`)
}

// GetMessageType parses a JSON string to retrieve the message-type
func GetMessageType(message string) (string, error) {
	submatches := messageTypeRegexp.FindStringSubmatch(message)
	if submatches == nil {
		return "", fmt.Errorf("no match on message-type '%s'", message)
	}

	return submatches[1], nil
}
