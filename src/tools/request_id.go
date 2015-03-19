package tools

import (
	"fmt"
	"regexp"
)

var (
	requestIDRegexp = regexp.MustCompile(`"user-request-id"\s*\:\s*"(.+?)"`)
)

// GetRequestID parses a JSON string to retrieve the user-request-id
func GetRequestID(message string) (string, error) {
	submatches := requestIDRegexp.FindStringSubmatch(message)
	if submatches == nil {
		return "", fmt.Errorf("no match on user-request-id '%s'", message)
	}

	return submatches[1], nil
}
