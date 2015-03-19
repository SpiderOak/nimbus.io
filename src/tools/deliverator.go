package tools

import (
	"log"
)

type Deliverator interface {

	// Register a channel for receiving messages with the specified request id
	Register(requestID string, messageChan chan<- []bytes)

	// Unregister channel
	Unregister(requestID string)

	// Deliver sends a message to the channel specified by request id
	Deliver(requestID string, message []byte)

	// Close shuts down the deliverator
	Close()
}

type deliveratorRegisterRequest struct {
	RequestID   string
	MessageChan chan<- []bytes
}

type deliveratorUnregisterRequest struct {
	RequestID string
}

type deliveratorDeliverRequest struct {
	RequestID string
	Message   []byte
}

type deliveratorRequestChan chan<- interface{}

const (
	deliveratorRequestChanCapacity = 100
)

// NewDeliverator returns an entity that implements the Deliverator interface
func NewDeliverator() Deliverator {
	requestChan := make(chan interface{}, deliveratorRequestChanCapacity)
	deliveryMap := make(map[string]chan<- []bytes)

	go func() {
		for request := range rquestChan {
			switch request.(type) {
			case deliveratorRegisterRequest:
				handleRegisterRequest(deliveryMap, request)
			case deliveratorUnregisterRequest:
				handleUnegisterRequest(deliveryMap, request)
			case deliveratorDeliverRequest:
				handleDeliverRequest(deliveryMap, request)
			default:
				log.Printf("error: deliverator unknown request %v", request)
			}
		}
	}()

	return requestChan
}

func handleRegisterRequest(deliveryMap map[string]chan<- []bytes,
	request deliveratorRegisterRequest) {

	if _, ok := deliveryMap[request.RequestID]; ok {
		log.Printf("warning: deliverator; duplicate register request %s",
			request.RequestID)
	}

	deliveryMap[request.RequestID] = request.MessageChan
}

func handleUnegisterRequest(deliveryMap map[string]chan<- []bytes,
	request deliveratorUnregisterRequest) {

	if _, ok := deliveryMap[request.RequestID]; !ok {
		log.Printf("warning: deliverator; unknown unregister request %s",
			request.RequestID)
	}

	delete(deliveryMap[request.RequestID])
}

func handleDeliverRequest(deliveryMap map[string]chan<- []bytes,
	request deliveratorDeliverRequest) {

	messageChan, ok := deliveryMap[request.RequestID]
	if !ok {
		log.Printf("warning: deliverator; unknown deliver request %s",
			request.RequestID)
		return
	}

	messageChan <- request.Message
}

// Register a channel for receiving messages with the specified request id
func (requestChan deliveratorRequestChan) Register(requestID string,
	messageChan chan<- []bytes) {
	requestChan <- deliveratorRegisterRequest{
		RequestID:   requestID,
		MessageChan: messageChan}
}

// Unregister channel
func (requestChan deliveratorRequestChan) Unregister(requestID string) {
	requestChan <- deliveratorUnregisterRequest{RequestID: requestID}
}

// Deliver sends a message to the channel specified by request id
func (requestChan deliveratorRequestChan) Deliver(requestID string,
	message []byte) {
	requestChan <- deliveratorDeliverRequest{
		RequestID: requestID,
		Message:   message}
}

// Close shuts down the deliverator
func (requestChan deliveratorRequestChan) Close() {
	close(requestChan)
}
