package main

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
	"encoding/json"
	"testing"
)

type StartConjoinedArchive struct {
	CollectionID    uint32 `json:"collection-id"`
	Key             string `json:"key"`
	UnifiedID       uint64 `json:"unified-id"`
	HandoffNodeName string `json:"handoff-node-name"`
}

const (
	testJSONString = `{"priority": 1402513731, "unified-id": 740661320822559745, "timestamp-repr": "datetime.datetime(2014, 6, 11, 19, 8, 51, 574370)", "key": "test_entire_multipart", "collection-id": 5555, "message-id": "d33181e8f19b11e383cd08002708c001", "message-type": "start-conjoined-archive", "user-request-id": "fbcb114c-730c-410a-a356-d8de3ea02dd6", "source-node-name": "dc0-01-06"}`
)

func TestParsingJSONMessage(t *testing.T) {
	var startConjoinedArchive StartConjoinedArchive

	err := json.Unmarshal([]byte(testJSONString), &startConjoinedArchive)
	if err != nil {
		t.Fatalf("json.Unmarshal failed %s", err)
	}

	t.Logf("handoff node name = '%s'", startConjoinedArchive.HandoffNodeName)

	if startConjoinedArchive.UnifiedID != uint64(740661320822559745) {
		t.Fatalf("invalid unified id %d", startConjoinedArchive.UnifiedID)
	}
}
