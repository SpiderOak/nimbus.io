package main

import (
	"log"

	"github.com/pebbe/zmq4"
)

// NewPullSocketHandler returns a function suitable for use as a handler
// by zmq.Reactor
func NewPullSocketHandler(pullSocket *zmq4.Socket) func(zmq4.State) error {
	return func(_ zmq4.State) error {
		log.Printf("debug: pull socket ready to read")
		return nil
	}
}
