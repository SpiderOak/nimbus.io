package tools

import (
	"encoding/json"
	"testing"

	"github.com/pebbe/zmq4"
)

const (
	testAddress      = "tcp://127.0.0.1:6000"
	uuid             = "4923d5f8e0f411e3aa6108002708c001"
	timestamp        = "datetime.datetime(2014, 5, 21, 18, 11, 9, 637377)"
	b64EncodedDigest = "LrC28p5vVOMInIkw3RFc5w=="
)

func TestHandshake(t *testing.T) {
	_, err := NewResilientServer(testAddress)
	if err != nil {
		t.Fatalf("NewResilientServer failed %s", err)
	}

	message := map[string]interface{}{
		"message-type": "resilient-server-handshake",
		"message-id":   uuid}

	reply, err := talkToServer(message)
	if err != nil {
		t.Fatalf("talkToServer: %s", err)
	}

	t.Logf("reply = %s", reply)

}

func TestArchiveKeyEntire(t *testing.T) {
	messageChan, err := NewResilientServer(testAddress)
	if err != nil {
		t.Fatalf("NewResilientServer failed %s", err)
	}

	message := map[string]interface{}{
		"message-type":       "archive-key-entire",
		"message-id":         uuid,
		"priority":           4,
		"collection-id":      "collection_id",
		"key":                "key",
		"timestamp-repr":     timestamp,
		"segment-num":        4,
		"segment-size":       1024,
		"segment-adler32":    32768,
		"segment-md5-digest": b64EncodedDigest,
		"file-size":          1024,
		"file-adler32":       32768,
		"file-hash":          b64EncodedDigest,
		"handoff-node-name":  nil}

	_, err = talkToServer(message)
	if err != nil {
		t.Fatalf("talkToServer: %s", err)
	}

	select {
	case chanMessage := <-messageChan:
		t.Logf("chanMessage = %s", chanMessage)
	default:
		t.Fatalf("no chanMessage fo archive-key-entire")
	}

}

func talkToServer(message map[string]interface{}) (Message, error) {
	socket, err := zmq4.NewSocket(zmq4.REQ)
	if err != nil {
		return nil, err
	}
	defer socket.Close()

	err = socket.Connect(testAddress)
	if err != nil {
		return nil, err
	}

	marshalledMessage, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	_, err = socket.SendMessage([]string{string(marshalledMessage)})
	if err != nil {
		return nil, err
	}

	rawMessage, err := socket.RecvMessage(0)
	if err != nil {
		return nil, err
	}

	var reply Message
	err = json.Unmarshal([]byte(rawMessage[0]), &reply)
	if err != nil {
		return nil, err
	}

	return reply, nil
}
