package router

import (
	"fmt"
	"strings"
)

// parseCollectionFromHostName returns the Nimbus.io collection name from host name
func parseCollectionFromHostName(hostName string) string {
	if !strings.HasSuffix(hostName, serviceDomain) {
		return ""
	}
	if hostName == serviceDomain {
		return ""
	}
	suffix := fmt.Sprintf(".%s", serviceDomain)
	if !strings.HasSuffix(hostName, suffix) {
		return ""
	}
	return strings.TrimSuffix(hostName, suffix)
}
