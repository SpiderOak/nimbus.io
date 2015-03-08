package access

import (
	"bytes"
	"encoding/json"
	"regexp"
	"testing"
)

type cleanseTestCase struct {
	Data          map[string]interface{}
	ExpectedValue AccessControlType
	ErrorRegexp   *regexp.Regexp
}

var (
	cleanseTestCases = []cleanseTestCase{
		cleanseTestCase{},
		cleanseTestCase{Data: map[string]interface{}{
			"x": bytes.Repeat([]byte("x"), maxAccessControlJSONLength+1)},
			ErrorRegexp: regexp.MustCompile(`^.*too large.*$`)},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "allow_unauth_read": "pork"},
			ExpectedValue: AccessControlType{},
			ErrorRegexp:   regexp.MustCompile(`^.*unable to cast to bool.*$`)},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "allow_unauth_read": true},
			ExpectedValue: AccessControlType{Version: "1.0",
				Entry: AccessControlEntry{AllowUnauthenticatedRead: true}}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "ipv4_whitelist": nil},
			ExpectedValue: AccessControlType{Version: "1.0"}}}
)

func TestAccessControlCleanse(t *testing.T) {
	for i, testCase := range cleanseTestCases {
		var err error
		var marshaledData []byte

		if testCase.Data == nil {
			marshaledData = nil
		} else {
			marshaledData, err = json.Marshal(testCase.Data)
			if err != nil {
				t.Fatalf("#%d unable to marshal test data %s", err)
			}
		}

		accessControl, err := LoadAccessControl(marshaledData)
		if err != nil {
			if testCase.ErrorRegexp == nil {
				t.Fatalf("%d unexpected error %s", i+1, err)
			}
			if !testCase.ErrorRegexp.MatchString(err.Error()) {
				t.Fatalf("%d unmatched error %s", i+1, err)
			}
		} else {
			if testCase.ErrorRegexp != nil {
				t.Fatalf("%d expected error found nil", i+1)
			}
			if !accessControl.Equal(testCase.ExpectedValue) {
				t.Fatalf("#%d value mismatch expecting %v found %v",
					i+1, testCase.ExpectedValue, accessControl)
			}
		}
	}
}

func TestAccessControlCheck(t *testing.T) {
}
