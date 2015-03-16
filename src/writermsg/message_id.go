package writermsg

import (
	"fmt"
	"regexp"
)

var (
	messageIDRegexp *regexp.Regexp
)

func init() {
	// "message-type": "start-conjoined-archive"
	messageIDRegexp = regexp.MustCompile(`"message-id"\s*\:\s*"(.+?)"`)
}

// GetMessageID parses a JSON string to retrieve the message-id
func GetMessageID(message string) (string, error) {
	submatches := messageIDRegexp.FindStringSubmatch(message)
	if submatches == nil {
		return "", fmt.Errorf("no match on message-id '%s'", message)
	}

	return submatches[1], nil
}
