package unifiedid

import (
	"testing"
)

func TestUnifiedIDFactory(t *testing.T) {
	const shardID uint32 = 42
	var err error
	var unifiedIDChan UnifiedIDChan
	var prevUnifiedID uint64

	if unifiedIDChan, err = NewUnifiedIDFactory(shardID); err != nil {
		t.Fatalf("NewUnifiedIDFactory failed %s", err)
	}

	for i := 0; i < 1000; i++ {
		unifiedID := <-unifiedIDChan
		if prevUnifiedID != 0 && unifiedID <= prevUnifiedID {
			t.Fatalf("#%d sequence error %d not greater than %d",
				i+1, unifiedID, prevUnifiedID)
		}
		prevUnifiedID = unifiedID
	}
}
