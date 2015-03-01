package tools

import (
	"fmt"
)

// RedisWebMonitorHashName returns the name of the redis HASH
// for storing and retrieving web monitor availability information
func RedisWebMonitorHashName(hostName string) string {
	return fmt.Sprintf("nimbus.io.web_monitor.%s", hostName)
}

// RedisWebMonitorHashKey returns the key used to access an individual
// server in the web monitor HASH
func RedisWebMonitorHashKey(address, port string) string {
	return fmt.Sprintf("%s:%s", address, port)
}
