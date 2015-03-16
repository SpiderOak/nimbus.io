package writermsg

import (
	"fmt"
	"regexp"
)

type MetaPair struct {
	Key   string
	Value string
}

func (m MetaPair) String() string {
	return fmt.Sprintf("{Key: '%s', Value: '%s'}", m.Key, m.Value)
}

var (
	metaRegexp *regexp.Regexp
)

func init() {
	// "message-type": "start-conjoined-archive"
	metaRegexp = regexp.MustCompile(`"__nimbus_io__(\S+?)"\s*\:\s*"*?([^, "]+)"*?`)
}

// GetMetaFromJSON parses a JSON string to retrieve meta data
func GetMetaFromJSON(message string) []MetaPair {
	var result []MetaPair
	submatches := metaRegexp.FindAllStringSubmatch(message, -1)
	for _, submatch := range submatches {
		result = append(result, MetaPair{Key: submatch[1], Value: submatch[2]})
	}
	return result
}
