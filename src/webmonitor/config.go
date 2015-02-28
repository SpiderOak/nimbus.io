package main

import (
	"encoding/json"
	"io"
)

type Config struct {
	Address        string `json:"address"`
	Port           string `json:"port"`
	Host           string `json:"host"`
	Path           string `json:"path"`
	Method         string `json:"method"`
	ExpectedStatus int    `json:"expected-status"`
	BodyTest       string `json:"body-test"`
	TimeoutSeconds int    `json:"timeout-seconds"`
}

func loadConfig(reader io.Reader) ([]Config, error) {
	var err error
	var result []Config

	decoder := json.NewDecoder(reader)
	if err = decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
