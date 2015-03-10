package access

import (
	"net"
	"testing"
)

type checkTestCase struct {
	RequestedAccessType AccessType
	AccessControl       AccessControlType
	Path                string
	RequesterIP         net.IP
	Referer             string
	ExpectedResult      AccessStatus
}

var (
	checkTestCases = []checkTestCase{
		checkTestCase{RequestedAccessType: Read,
			AccessControl:  AccessControlType{},
			ExpectedResult: RequiresPasswordAuthentication},
		checkTestCase{RequestedAccessType: Read,
			AccessControl: AccessControlType{Version: "1.0",
				AccessControlEntry: AccessControlEntry{AllowUnauthenticatedRead: true}},
			ExpectedResult: Allowed},
	}
)

func TestAccessControlCheck(t *testing.T) {
	for i, testCase := range checkTestCases {
		var err error
		var result AccessStatus

		result, err = CheckAccess(testCase.RequestedAccessType,
			testCase.AccessControl, testCase.Path, testCase.RequesterIP,
			testCase.Referer)
		if err != nil {
			t.Fatalf("#%d CheckAccess returned error %s", i+1, err)
		}
		if result != testCase.ExpectedResult {
			t.Fatalf("#%d access mismatch: expecting %s found %s",
				i+1, testCase.ExpectedResult, result)
		}
	}
}
