package access

import (
	"fmt"
	"log"
	"net"
	"strings"
)

const (
	CurrentAccessControlVersion = "1.0"
)

// CheckAccess returns AccessStatus based on what access the user is allowed
func CheckAccess(requestedAccess AccessType,
	accessControl AccessControlType, path string,
	requesterIP net.IP, referer string) (AccessStatus, error) {

	// if no special access control is specified, we must authenticate
	if accessControl.Equal(AccessControlType{}) {
		return RequiresPasswordAuthentication, nil
	}

	if accessControl.Version != CurrentAccessControlVersion {
		return Forbidden, fmt.Errorf("version mismatch: expected %s found %s",
			CurrentAccessControlVersion, accessControl.Version)
	}

	normalizedPath := normalizePath(path)
	accessControlEntry := accessControl.AccessControlEntry
	for i, locationEntry := range accessControl.Locations {
		if locationEntry.Prefix != "" {
			normalizedPrefix := normalizePath(locationEntry.Prefix)
			if strings.HasPrefix(normalizedPath, normalizedPrefix) {
				log.Printf("using location #%d prefix %s matches %s",
					i+1, locationEntry.Prefix, path)
				accessControlEntry = locationEntry.AccessControlEntry
				break
			}
		}
		if locationEntry.Regexp != nil {
			if locationEntry.Regexp.MatchString(path) {
				log.Printf("using location #%d regexp matches %s",
					i+1, path)
				accessControlEntry = locationEntry.AccessControlEntry
				break
			}
		}
	}

	/*
		ipv4_whitelist applies to ALL requests.
		Specifying an ipv4_whitelist means that ALL requests,
		unauthenticated or authenticated,
		must be specifically included in the white list,
		or they will be rejected.
	*/
	if accessControlEntry.IPv4WhiteList != nil {
		inWhitelist := false
		for i, whitelistEntry := range accessControlEntry.IPv4WhiteList {
			if whitelistEntry.Contains(requesterIP) {
				log.Printf("debug: requesterIP %s in whitelist entry #%d %s",
					requesterIP, i+1, whitelistEntry)
				inWhitelist = true
				break
			}
		}
		if !inWhitelist {
			return Forbidden, nil
		}
	}

	/*
	   The unauth_referrer_whitelist is a further restriction on which URLs
	   an unauthenticated request claim to be originating from.
	   It has no effect on authenticated requests. Just because a request meets
	   the requirements of unauth_referrer_whitelist alone does not mean it is
	   automatically allowed. It is allowed only if it should be allowed
	   according to allow_unauth_read, allow_unauth_write, etc.
	*/
	allowUnauthReferrers = true
	if accessControlEntry.UnauthReferrerWhitelist != nil {
		allowUnauthReferrers = false
		for i, whiteListEntry := range accessControlEntry.UnauthReferrerWhitelist {

		}
	}

	return Forbidden, fmt.Errorf("Not implemented")
}

func normalizePath(path string) string {
	return strings.ToLower(strings.TrimLeft(path, "/"))
}
