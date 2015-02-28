package main

import (
	"bytes"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	var testData = []byte(`


[
{
   "address": "10.0.3.14",
   "port": "9002",
   "host": "nimbusio-head-simdc2-1",
   "path": "/ping",
   "method": "get",
   "expected-status": 200,
   "body-test": "^ok$",
   "timeout-seconds": 10
}
,
{
   "address": "10.0.3.15",
   "port": "9002",
   "host": "nimbusio-head-simdc2-2",
   "path": "/ping",
   "method": "get",
   "expected-status": 200,
   "body-test": "^ok$",
   "timeout-seconds": 10
}
,
{
   "address": "10.0.3.16",
   "port": "9000",
   "host": "sim-ash1",
   "path": "/ping",
   "method": "get",
   "expected-status": 200,
   "body-test": "^ok$",
   "timeout-seconds": 10
}
]`)
	reader := bytes.NewBuffer(testData)
	config, err := loadConfig(reader)
	if err != nil {
		t.Fatalf("error in loadConfig %s", err)
	}
	if len(config) != 3 {
		t.Fatalf("wrong config len expected %d found %d", 3, len(config))
	}
	entry := config[0]
	if entry.Address != "10.0.3.14" {
		t.Fatalf("invalid address %s", entry.Address)
	}
	if entry.TimeoutSeconds != 10 {
		t.Fatalf("invalid TimeoutSeconds %d", entry.TimeoutSeconds)
	}
}
