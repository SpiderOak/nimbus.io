package tools

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var (
	timestampReprRegexp *regexp.Regexp
)

func init() {
	//  datetime.datetime(2011, 6, 30, 13, 52, 34, 720271)
	timestampReprRegexp = regexp.MustCompile(`^datetime.datetime\((\d{4}),\s(\d{1,2}),\s(\d{1,2}),\s(\d{1,2}),\s(\d{1,2}),\s(\d{1,2}),\s(\d+)\)$`)
}

// ParseTimestampRepr parses the python string of repr(datetime.utcnow()) and
// returns a time to the second
func ParseTimestampRepr(timestampRepr string) (time.Time, error) {
	var err error
	var nilTime time.Time
	var year, month, day, hour, min, sec int

	submatches := timestampReprRegexp.FindStringSubmatch(timestampRepr)
	if submatches == nil {
		return nilTime, fmt.Errorf("unable to parse time repr %s",
			timestampRepr)
	}

	if year, err = strconv.Atoi(submatches[1]); err != nil {
		return nilTime, err
	}

	if month, err = strconv.Atoi(submatches[2]); err != nil {
		return nilTime, err
	}

	if day, err = strconv.Atoi(submatches[3]); err != nil {
		return nilTime, err
	}

	if hour, err = strconv.Atoi(submatches[4]); err != nil {
		return nilTime, err
	}

	if min, err = strconv.Atoi(submatches[5]); err != nil {
		return nilTime, err
	}

	if sec, err = strconv.Atoi(submatches[6]); err != nil {
		return nilTime, err
	}

	timestamp := time.Date(year, time.Month(month), day, hour, min, sec,
		0, time.UTC)

	return timestamp, nil
}
