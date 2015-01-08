package tools

import (
	"fmt"
	"net"
	"strings"
)

type HostResolver interface {

	// Lookup the hostname, return address as string, or error
	Lookup(hostName string) (string, error)
}

type resolveCache map[string]string

type resolveRequest struct {
	HostName  string
	ReplyChan chan<- interface{}
}

type resolveImpl struct {
	RequestChan chan<- resolveRequest
}

// NewHostResolver returns an entity that implements the HostResolver interface
func NewHostResolver() HostResolver {
	cache := make(resolveCache)
	requestChan := make(chan resolveRequest, 10)

	go func() {
		var address string
		var ok bool
		var err error

	REQUEST_LOOP:
		for request := range requestChan {
			if address, ok = cache[request.HostName]; !ok {
				var addressSlice []string
				if addressSlice, err = net.LookupHost(request.HostName); err != nil {
					request.ReplyChan <- fmt.Errorf("net.LookupHost %s", err)
					continue REQUEST_LOOP
				}

				var foundAddress bool
				for _, possibleAddress := range addressSlice {
					// test for an ipv4 address
					if strings.Count(possibleAddress, ".") == 3 {
						address = possibleAddress
						foundAddress = true
						break
					}
				}

				if !foundAddress {
					request.ReplyChan <- fmt.Errorf(
						"net.LookupHost %d entries %v",
						len(addressSlice), addressSlice)
					continue REQUEST_LOOP
				}

				cache[request.HostName] = address
			}

			request.ReplyChan <- address
		}
	}()

	return resolveImpl{RequestChan: requestChan}
}

func (r resolveImpl) Lookup(hostName string) (string, error) {
	replyChan := make(chan interface{})
	r.RequestChan <- resolveRequest{HostName: hostName, ReplyChan: replyChan}
	rawReply := <-replyChan
	switch reply := rawReply.(type) {
	case error:
		return "", reply
	case string:
		return reply, nil
	}
	return "", fmt.Errorf("unknown reply %T %v", rawReply, rawReply)
}
