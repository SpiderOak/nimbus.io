package access

import (
	"bytes"
	"encoding/json"
	"net"
	"regexp"
	"testing"
	"types"
)

type cleanseTestCase struct {
	Data          map[string]interface{}
	ExpectedValue AccessControlType
	ErrorRegexp   *regexp.Regexp
}

type checkTestCase struct {
	AccessType     types.AccessType
	AccessControl  AccessControlType
	ExpectedResult types.AccessStatus
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
				AccessControlEntry: AccessControlEntry{AllowUnauthenticatedRead: true}}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "ipv4_whitelist": nil},
			ExpectedValue: AccessControlType{Version: "1.0"}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "ipv4_whitelist": "clam"},
			ExpectedValue: AccessControlType{},
			ErrorRegexp:   regexp.MustCompile(`^.*unable to cast.*$`)},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "ipv4_whitelist": []string{}},
			ExpectedValue: AccessControlType{Version: "1.0"}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "ipv4_whitelist": []string{"192.168.1.102"}},
			ExpectedValue: AccessControlType{Version: "1.0",
				AccessControlEntry: AccessControlEntry{IPv4WhiteList: []net.IPNet{
					net.IPNet{IP: net.ParseIP("192.168.1.102"), Mask: net.CIDRMask(32, 32)}}}}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "unauth_referrer_whitelist": nil},
			ExpectedValue: AccessControlType{Version: "1.0"}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "unauth_referrer_whitelist": "clam"},
			ExpectedValue: AccessControlType{},
			ErrorRegexp:   regexp.MustCompile(`^.*unable to cast.*$`)},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "unauth_referrer_whitelist": []string{}},
			ExpectedValue: AccessControlType{Version: "1.0"}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "unauth_referrer_whitelist": []string{"example.com/myapp"}},
			ExpectedValue: AccessControlType{Version: "1.0",
				AccessControlEntry: AccessControlEntry{UnauthReferrerWhitelist: []string{"example.com/myapp"}}}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "locations": nil},
			ExpectedValue: AccessControlType{Version: "1.0"}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "locations": "clam"},
			ExpectedValue: AccessControlType{},
			ErrorRegexp:   regexp.MustCompile(`^.*unable to cast.*$`)},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "locations": []interface{}{map[string]interface{}{}}},
			ExpectedValue: AccessControlType{Version: "1.0"}},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "locations": []interface{}{map[string]interface{}{"regexp": "["}}},
			ExpectedValue: AccessControlType{},
			ErrorRegexp:   regexp.MustCompile(`^.*unable to compile.*$`)},
		cleanseTestCase{Data: map[string]interface{}{
			"version": "1.0", "locations": []interface{}{map[string]interface{}{"prefix": "aaa",
				"allow_unauth_read": true}}},
			ExpectedValue: AccessControlType{Version: "1.0",
				Locations: []LocationEntry{LocationEntry{Prefix: "aaa",
					AccessControlEntry: AccessControlEntry{AllowUnauthenticatedRead: true}}}}},
	}

	checkTestCases = []checkTestCase{
		checkTestCase{AccessType: types.ReadAccess, 
			AccessControl: AccessControlType{}
			ExpectedResult: },
	}
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
				t.Fatalf("#%d unexpected error %s", i+1, err)
			}
			if !testCase.ErrorRegexp.MatchString(err.Error()) {
				t.Fatalf("#%d unmatched error %s", i+1, err)
			}
		} else {
			if testCase.ErrorRegexp != nil {
				t.Fatalf("#%d expected error found nil", i+1)
			}
			if !accessControl.Equal(testCase.ExpectedValue) {
				t.Fatalf("#%d value mismatch expecting %v found %v",
					i+1, testCase.ExpectedValue, accessControl)
			}
		}
	}
}

func TestAccessControlCheck(t *testing.T) {
	for i, testCase := range checkTestCases {
		var err error
	}
}
