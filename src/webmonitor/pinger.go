package main

import (
	"fmt"
	"log"
	"time"
)

const pollingInterval = time.Duration(3) * time.Second

func pinger(config Config) {
	var timeoutInterval = time.Duration(config.TimeoutSeconds) * time.Second
	var label = fmt.Sprintf("%s:%s", config.Address, config.Port)

	log.Printf("info: pinging %s %s every %v timeout interval = %v",
		config.Host, label, pollingInterval, timeoutInterval)

	time.Sleep(time.Duration(5) * time.Second)

}
