package msg

/*
python message dict

{'priority': 1402513731,
 'key': u'test_entire_multipart',
 'unified-id': 740661320822559745,
 'timestamp-repr': 'datetime.datetime(2014, 6, 11, 19, 8, 51, 574370)',
 'message-type': 'start-conjoined-archive',
 'collection-id': 5555, 'message-id': 'd33181e8f19b11e383cd08002708c001',
 'user-request-id': 'fbcb114c-730c-410a-a356-d8de3ea02dd6',
 'source-node-name': 'dc0-01-06'}
*/

import (
	"testing"
)

func TestGetMessageType(t *testing.T) {
	testJSONString := `{"priority": 1402513731, "unified-id": 740661320822559745, "timestamp-repr": "datetime.datetime(2014, 6, 11, 19, 8, 51, 574370)", "key": "test_entire_multipart", "collection-id": 5555, "message-id": "d33181e8f19b11e383cd08002708c001", "message-type": "start-conjoined-archive", "user-request-id": "fbcb114c-730c-410a-a356-d8de3ea02dd6", "source-node-name": "dc0-01-06"}`
	messageType, err := GetMessageType(testJSONString)
	if err != nil {
		t.Fatalf("GetMessageType %s", err)
	}

	if messageType != "start-conjoined-archive" {
		t.Fatalf("invalid message type '%s'", messageType)
	}
}
