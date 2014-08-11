package avail

import (
	"fmt"
	"net"
	"os"

	"github.com/garyburd/redigo/redis"

	"fog"
	"tools"
)

type availability struct {
	RedisWebMonitorHash string
	HostAddress         string
	ResolveCache        map[string]string
}

// Availability returns an entity that implements the Availability
// interface. .
func NewAvailability() (Availability, error) {
	var a availability
	var hostName string
	var err error

	if hostName, err = os.Hostname(); err != nil {
		return a, err
	}

	a.RedisWebMonitorHash = fmt.Sprintf("nimbus.io.web_monitor.%s", hostName)

	hostAddresses, err := net.LookupHost(hostName)
	if err != nil {
		return a, err
	}

	if len(hostAddresses) != 1 {
		return a, fmt.Errorf("expecting 1 address %q", hostAddresses)
	}

	a.HostAddress = hostAddresses[0]
	a.ResolveCache = make(map[string]string)

	fog.Info("Avaialbility for host %s %s", hostName, a.HostAddress)
	return a, nil
}

func (a availability) AvailableHosts(hostNames []string, destPort string) (
	[]string, error) {
	var err error

	conn, err := tools.GetRedisConnection()
	if err != nil {
		return nil, err
	}

	var availHosts []string
	for _, hostName := range hostNames {
		var address string
		var ok bool
		var err error

		if address, ok = a.ResolveCache[hostName]; !ok {
			var addressSlice []string
			if addressSlice, err = net.LookupHost(hostName); err != nil {
				fog.Warn("Host %s not available: net.LookupHost %s", hostName, err)
				continue
			}
			if len(addressSlice) != 1 {
				fog.Warn("Host %s not available: net.LookupHost %q", hostName, addressSlice)
				continue
			}
			a.ResolveCache[hostName] = addressSlice[0]
		}

		addressKey := fmt.Sprintf("%s:%s", address, destPort)

		availJSON, err := redis.Bytes(conn.Do("HGET", a.RedisWebMonitorHash, addressKey))
		if err != nil {
			fog.Warn("Host %s not available: '%s %s %s' %s", hostName,
				"HGET", a.RedisWebMonitorHash, addressKey, err)
			continue
		}

		hostAvail, err := parseAvailJSON(availJSON)
		if err != nil {
			fog.Warn("Host %s not available: unable to parse JSON %q %s",
				hostName, availJSON, err)
			continue
		}

		if hostAvail.Reachable {
			availHosts = append(availHosts, hostName)
		} else {
			fog.Warn("host %s listed as unreachable %f", hostName,
				hostAvail.TimestampFloat)
		}
	}

	if len(availHosts) == 0 {
		fog.Warn("unable to find available hosts, using all")
		return hostNames, nil
	}

	// to start with, just make all hosts available
	return availHosts, nil
}
