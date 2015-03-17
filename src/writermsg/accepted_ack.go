package writermsg

import (
	"fmt"
	"regexp"
)

var (
	acceptedRegexp *regexp.Regexp
)

func init() {
	// "message-type": "start-conjoined-archive"
	acceptedRegexp = regexp.MustCompile(`"accepted"\s*\:\s*"(.+?)"`)
}

// GetMessageID parses a JSON string to retrieve the message-id
func GetAckAccepted(message string) (bool, error) {
	submatches := acceptedRegexp.FindStringSubmatch(message)
	if submatches == nil {
		return false, fmt.Errorf("no match on accepted '%s'", message)
	}

	return submatches[1] == "true", nil
}
