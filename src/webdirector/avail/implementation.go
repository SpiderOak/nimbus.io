package avail

import (
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"

	"fog"
	"tools"
)

type availability struct {
	RedisWebMonitorHash string
	HostAddress         string
	HostResolver        tools.HostResolver
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

	a.HostResolver = tools.NewHostResolver()
	if a.HostAddress, err = a.HostResolver.Lookup(hostName); err != nil {
		return a, err
	}

	fog.Info("Availability for host %s %s", hostName, a.HostAddress)
	return a, nil
}

func (a availability) AvailableHosts(hostNames []string, destPort string) (
	[]string, error) {

	var availHosts []string
	for _, hostName := range hostNames {
		var address string
		var err error

		if address, err = a.HostResolver.Lookup(hostName); err != nil {
			fog.Warn("Host %s not available: %s", hostName, err)
			continue
		}

		addressKey := fmt.Sprintf("%s:%s", address, destPort)

		availJSON, err := redis.Bytes(tools.RedisDo("HGET",
			a.RedisWebMonitorHash, addressKey))
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
