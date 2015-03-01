package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const pollingInterval = time.Duration(3) * time.Second

func pinger(haltChan <-chan struct{},
	hostAvailChan chan<- HostAvailForAddress,
	config Config) {

	var err error
	var timeoutInterval = time.Duration(config.TimeoutSeconds) * time.Second
	var label = fmt.Sprintf("%s:%s", config.Address, config.Port)

	log.Printf("info: pinging %s %s every %v timeout interval = %v",
		config.Host, label, pollingInterval, timeoutInterval)

	for {
		if err = ping(config); err != nil {
			log.Printf("%s: ping failed: %s", label, err)
		}

		time.Sleep(pollingInterval)

		select {
		case _, _ = <-haltChan:
			// the only way we get something from this channel is when it closes
			log.Printf("debug: haltChan closed")
			return
		default:
		}
	}
}

func ping(config Config) error {
	var err error
	var client *http.Client
	var path string
	var url string
	var request *http.Request
	var response *http.Response
	var body []byte
	var matched bool

	if strings.HasPrefix("/", config.Path) {
		path = config.Path[1:]
	} else {
		path = config.Path
	}

	url = fmt.Sprintf("http://%s:%s/%s", config.Address, config.Port, path)

	if request, err = http.NewRequest(config.Method, url, nil); err != nil {
		return err
	}

	client = &http.Client{
		Timeout: time.Duration(config.TimeoutSeconds) * time.Second}

	response, err = client.Do(request)
	if err != nil {
		return err
	}
	if response.Body != nil {
		defer response.Body.Close()
	}
	if response.StatusCode != config.ExpectedStatus {
		return fmt.Errorf("expected status %d received status %d %s",
			config.ExpectedStatus, response.StatusCode, response.Status)
	}

	if body, err = ioutil.ReadAll(response.Body); err != nil {
		return err
	}

	matched, err = regexp.MatchString(config.BodyTest, string(body))
	if err != nil {
		return err
	}
	if !matched {
		return fmt.Errorf("body mismatch %s %s", config.BodyTest, body)
	}

	return nil
}
