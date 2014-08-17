package tools

import (
	"fmt"
	"os"

	"github.com/bradfitz/gomemcache/memcache"

	"fog"
)

func NewMemcacheClient() *memcache.Client {
	memcacheHost := os.Getenv("NIMBUSIO_MEMCACHED_HOST")
	if memcacheHost == "" {
		memcacheHost = "localhost"
	}

	memcachePort := os.Getenv("NIMBUSIO_MEMCACHED_PORT")
	if memcachePort == "" {
		memcachePort = "11211"
	}

	memcacheAddress := fmt.Sprintf("%s:%s", memcacheHost, memcachePort)

	fog.Info("NewMemcacheClient connecting to %s", memcacheAddress)

	return memcache.New(memcacheAddress)
}
