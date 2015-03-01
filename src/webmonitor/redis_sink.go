package main

import (
	"encoding/json"
	"log"
	"os"

	"tools"
)

type HostAvailForAddress struct {
	tools.HostAvail
	Address string
	Port    string
}

const (
	HostAvailChanCapacity = 10
)

// NewRedisHostAvailSink returns a channel for HostAvailForAddress to
// store tools.HostAvail structs in REDIS
func NewRedisHostAvailSink() (chan<- HostAvailForAddress, error) {
	var err error
	var hostName string
	var hashName string
	var key string
	var data []byte

	hostAvailChan := make(chan HostAvailForAddress, HostAvailChanCapacity)

	if hostName, err = os.Hostname(); err != nil {
		return hostAvailChan, err
	}
	hashName = tools.RedisWebMonitorHashName(hostName)

	go func() {
		for item := range hostAvailChan {
			key = tools.RedisWebMonitorHashKey(item.Address, item.Port)
			data, err = json.Marshal(item.HostAvail)
			if err != nil {
				log.Printf("error: error marshaling %v; %s",
					item.HostAvail, err)
				continue
			}
			if _, err = tools.RedisDo("HSET", hashName, key, data); err != nil {
				log.Printf("error: redis HSET failed %s %s %s",
					hashName, key, err)
				continue
			}
		}
	}()
	return hostAvailChan, nil
}
