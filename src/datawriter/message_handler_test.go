package main

/*
python message dict

{'priority': 1402513731,
 'key': u'test_entire_multipart',
 'unified-id': 740661320822559745,
 'timestamp-repr': 'datetime.datetime(2014, 6, 11, 19, 8, 51, 574370)',
 'message-type': 'start-conjoined-archive',
 'collection-id': 5555, 'message-id': 'd33181e8f19b11e383cd08002708c001',
 'user-request-id': 'fbcb114c-730c-410a-a356-d8de3ea02dd6'}
*/

import (
	"encoding/json"
	"testing"

	"datawriter/types"
)

const (
	testJSONString = `{"priority": 1402513731, "unified-id": 740661320822559745, "timestamp-repr": "datetime.datetime(2014, 6, 11, 19, 8, 51, 574370)", "key": "test_entire_multipart", "collection-id": 5555, "message-id": "d33181e8f19b11e383cd08002708c001", "message-type": "start-conjoined-archive", "user-request-id": "fbcb114c-730c-410a-a356-d8de3ea02dd6"}`
)

func TestParsingJSONMessage(t *testing.T) {
	var messageMap types.MessageMap
	var err error
	var ok bool
	var floatUnifiedID float64
	var unifiedID uint64

	err = json.Unmarshal([]byte(testJSONString), &messageMap)
	if err != nil {
		t.Fatalf("json.Unmarshal failed %s", err)
	}

	if floatUnifiedID, ok = messageMap["unified-id"].(float64); !ok {
		t.Fatalf("unparseable unified-id %T, %s", messageMap["unified-id"],
			messageMap["unified-id"])
	}

	t.Logf("floatUnifiedID =  %f", floatUnifiedID)

	unifiedID = uint64(floatUnifiedID)

	if unifiedID != uint64(740661320822559745) {
		t.Fatalf("invalid unified id %d", unifiedID)
	}
}
