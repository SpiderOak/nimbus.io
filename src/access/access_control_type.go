package access

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
)

type AccessControlEntry struct {
	// if True, GET and HEAD requests for objects do not
	// require authentication
	AllowUnauthenticatedRead bool

	// if True, PUT and POST requests for objects do not
	// require authentication
	AllowUnauthenticatedWrite bool

	// if True, LISTMATCH does not require authentication
	AllowUnauthenticatedList bool

	// if True, DELETE requests do not require authentication
	AllowUnauthenticatedDelete bool

	/*
		if not null, must be a list of strings describing IPv4
		addresses of the form "W.X.Y.Z" or netblocks of the
		form "W.X.Y.Z/N" . Any request not originating from
		within a listed netblock will be failed with HTTP
		403.
	*/
	IPv4WhiteList []net.IPNet

	/*
		if not null, must be a list of strings.  Any request
		that does not include a HTTP Referer (sic) header
		prefixed with one the strings in the whitelist will be
		rejected with HTTP 403.
		The prefix refers to the part immediately after the
		schema.  For example, if unauth_referrer_whitelist was
		set to :
		[ "example.com/myapp" ]
		then a request with a Referer header of
		http://example.com/myapp/login would be allowed.
	*/
	UnauthReferrerWhitelist []string
}

type LocationEntry struct {
	Prefix string
	Regexp *regexp.Regexp
	Entry  AccessControlEntry
}

// AccessControlType is a struct loaded loaded from the JSON value found
// in the access_control column of the collection row in the central db
type AccessControlType struct {
	Version string

	// All applies to all reuests not overridden by location
	Entry AccessControlEntry

	/*
		the locations property allows a way to specify more
		fine grained access controls.  if present, it must be
		a list of objects, and the first object found to be
		matching the request will be used to define the
		access controls for the request.  if no match is
		found, the default settings specified for the
		collection in general (i.e. outside the 'locations'
		property) will be used.

		each object in the locations list must have either a
		"prefix" or a "regexp" property.  this will be used
		to see if an incoming request is subject to the
		objects access controls.  each object in the list may
		then have zero or more of the above properties
		allowed for access control (i.e. allow_unauth_read,
		ipv4_whitelist, etc.)

		In this way it is possible to have fine grained
		access controls over different parts of a collection.
		For example:
		{
		  "allow_unauth_read": false,
		  "ipv4_whitelist": null,
		  "prefix": "/abc"
		},
		{
		  "allow_unauth_read": false,
		  "ipv4_whitelist": null,
		  "prefix": "/def"
		}
	*/
	Locations []LocationEntry
}

const (
	maxAccessControlJSONLength = 16 * 1024
)

// LoadAccessControl loads and validates the access control struct
func LoadAccessControl(marshalledAccessControl []byte) (AccessControlType, error) {
	var result AccessControlType
	var unmarshaledMap map[string]interface{}
	var err error

	if marshalledAccessControl == nil {
		return result, nil
	}

	if len(marshalledAccessControl) > maxAccessControlJSONLength {
		return result, fmt.Errorf("JSON text too large %d bytes",
			len(marshalledAccessControl))
	}

	if err = json.Unmarshal(marshalledAccessControl, &unmarshaledMap); err != nil {
		return result, err
	}

	log.Printf("unmarshaledMap = %Q", unmarshaledMap)
	for key := range unmarshaledMap {

		rawData := unmarshaledMap[key]

		switch key {
		case "version":
			result.Version, err = castToString(rawData)
			if err != nil {
				return result, fmt.Errorf("version: %s", err)
			}
		case "allow_unauth_read":
			result.Entry.AllowUnauthenticatedRead, err = castToBool(rawData)
			if err != nil {
				return result, fmt.Errorf("allow_unauth_read: %s", err)
			}
		case "allow_unauth_write":
			result.Entry.AllowUnauthenticatedWrite, err = castToBool(rawData)
			if err != nil {
				return result, fmt.Errorf("allow_unauth_write: %s", err)
			}
		case "allow_unauth_list":
			result.Entry.AllowUnauthenticatedList, err = castToBool(rawData)
			if err != nil {
				return result, fmt.Errorf("allow_unauth_list: %s", err)
			}
		case "allow_unauth_delete":
			result.Entry.AllowUnauthenticatedDelete, err = castToBool(rawData)
			if err != nil {
				return result, fmt.Errorf("allow_unauth_delete: %s", err)
			}
		case "ipv4_whitelist":
			result.Entry.IPv4WhiteList, err = parseIPV4Whitelist(rawData)
			if err != nil {
				return result, fmt.Errorf("ipv4_whitelist: %s", err)
			}
		case "unauth_referrer_whitelist":
			result.Entry.UnauthReferrerWhitelist, err = parseSliceOfString(rawData)
			if err != nil {
				return result, fmt.Errorf("unauth_referrer_whitelist: %s", err)
			}
		case "locations":
			result.Locations, err = parseLocations(rawData)
			if err != nil {
				return result, fmt.Errorf("locations: %s", err)
			}
		default:
			return result, fmt.Errorf("Unknown key '%s'", key)
		}
	}

	return result, err
}

// Equal is defined for convenience in testing
func (a AccessControlEntry) Equal(b AccessControlEntry) bool {
	if a.AllowUnauthenticatedRead != b.AllowUnauthenticatedRead {
		return false
	}

	if a.IPv4WhiteList != nil {
		if b.IPv4WhiteList == nil {
			return false
		}

		if len(a.IPv4WhiteList) != len(b.IPv4WhiteList) {
			return false
		}

		for i := 0; i < len(a.IPv4WhiteList); i++ {
			if a.IPv4WhiteList[i].String() != b.IPv4WhiteList[i].String() {
				log.Printf("debug: whitelist #%d mismatch %s != %s",
					i+1, a.IPv4WhiteList[i].String(), b.IPv4WhiteList[i].String())
				return false
			}
		}
	} else {
		if b.IPv4WhiteList != nil {
			return false
		}
	}

	return true
}

// Equal is defined for convenience in testing
func (a AccessControlType) Equal(b AccessControlType) bool {
	if a.Version != b.Version {
		return false
	}

	if !a.Entry.Equal(b.Entry) {
		return false
	}

	return true
}

func castToString(rawData interface{}) (string, error) {
	s, ok := rawData.(string)
	if !ok {
		return "", fmt.Errorf("unable to cast to string")
	}
	return s, nil
}

func castToBool(rawData interface{}) (bool, error) {
	b, ok := rawData.(bool)
	if !ok {
		return false, fmt.Errorf("unable to cast to bool")
	}
	return b, nil
}

func parseIPV4Whitelist(rawData interface{}) ([]net.IPNet, error) {
	var whiteList []net.IPNet

	if rawData == nil {
		return whiteList, nil
	}

	b, ok := rawData.([]interface{})
	if !ok {
		return whiteList, fmt.Errorf("unable to cast to []interface{}")
	}
	for i, rawEntry := range b {
		s, ok := rawEntry.(string)
		if !ok {
			return whiteList, fmt.Errorf("unable to cast #%d to string", i+1)
		}

		stringSlice := strings.Split(s, "/")
		switch len(stringSlice) {
		case 1: // 192.168.1.1
			ip := net.ParseIP(s)
			mask := net.CIDRMask(32, 32)
			whiteList = append(whiteList, net.IPNet{ip, mask})
		case 2: // 192.168.1.1/24
			ip := net.ParseIP(stringSlice[0])
			n, err := strconv.Atoi(stringSlice[1])
			if err != nil {
				return whiteList, fmt.Errorf("%d non-numeric mask %Q %s",
					i+1, s, err)
			}
			mask := net.CIDRMask(n, 32)
			whiteList = append(whiteList, net.IPNet{ip, mask})
		default:
			return whiteList, fmt.Errorf("unparsable #%d %Q", i+1, s)
		}
	}
	return whiteList, nil
}

func parseSliceOfString(rawData interface{}) ([]string, error) {
	var stringSlice []string

	if rawData == nil {
		return stringSlice, nil
	}

	b, ok := rawData.([]interface{})
	if !ok {
		return stringSlice, fmt.Errorf("unable to cast to []interface{}")
	}
	for i, rawString := range b {
		s, ok := rawString.(string)
		if !ok {
			return stringSlice, fmt.Errorf("unable to cast #%d to string", i)
		}
		stringSlice = append(stringSlice, s)
	}

	return stringSlice, nil
}

func parseLocations(rawData interface{}) ([]LocationEntry, error) {
	var locations []LocationEntry
	var err error

	if rawData == nil {
		return locations, nil
	}

	b, ok := rawData.([]interface{})
	if !ok {
		return locations, fmt.Errorf("unable to cast to []interface{}")
	}

	for i, rawLocation := range b {
		unmarshaledMap, ok := rawLocation.(map[string]interface{})
		if !ok {
			return locations, fmt.Errorf("unable to cast to map[string]interface{}")
		}

		if len(unmarshaledMap) == 0 {
			return locations, nil
		}

		var locationEntry LocationEntry
		for key := range unmarshaledMap {

			rawLocationData := unmarshaledMap[key]

			switch key {
			case "prefix":
				locationEntry.Prefix, err = castToString(rawLocationData)
				if err != nil {
					return locations, fmt.Errorf("#%d %s %s", i+1, key, err)
				}
			case "regexp":
				rawRegexp, err := castToString(rawLocationData)
				if !ok {
					return locations, fmt.Errorf("#%d %s %s", i+1, key, err)
				}
				locationEntry.Regexp, err = regexp.Compile(rawRegexp)
				if err != nil {
					return locations, fmt.Errorf("#%d %s error unable to compile %s",
						i+1, key, err)
				}
			}
		}
		log.Printf("debug: #%d %s", i+1, unmarshaledMap)
	}

	return locations, fmt.Errorf("locations not implemented")
}
