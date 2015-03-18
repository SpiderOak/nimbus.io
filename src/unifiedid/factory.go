package unifiedid

import (
	"fmt"
	"time"
)

// UnifiedIDChan yields a stream of unified ids
type UnifiedIDChan <-chan uint64

const (
	unifiedIDChanCapacity = 1

	maxShardID = 0x1fff // 13 bits for shard id

	// XXX could use the founding of SpiderOak
	instagramEpoch uint64 = 1314220021721
)

// NewUnifiedIDFactory returns a channel that dtreams unified ids
func NewUnifiedIDFactory(shardID uint32) (UnifiedIDChan, error) {
	if shardID > maxShardID {
		return nil, fmt.Errorf("shard id %d too large", shardID)
	}

	unifiedIDChan := make(chan uint64, unifiedIDChanCapacity)

	go func() {
		var shiftedShardID uint64
		var prevMillisecond uint64
		var sequence uint64

		shiftedShardID = uint64(shardID) << (64 - 41 - 13)

		for {
			currentMillisecond := uint64(time.Now().UnixNano() / (1000 * 1000))
			if currentMillisecond != prevMillisecond {
				sequence = 0
				prevMillisecond = currentMillisecond
			}

			millisecondsSinceEpoch := currentMillisecond - instagramEpoch

			// fill the left-most 41 bits into an unsigned integer
			nextID := millisecondsSinceEpoch << (64 - 41)

			// fill the next 13 bits with shard-id
			nextID |= shiftedShardID

			// use our sequence to fill out the remaining bits.
			// mod by 1024 (so it fits in 10 bits)
			sequence += 1
			nextID |= (sequence % 1024)

			unifiedIDChan <- nextID
		}
	}()

	return unifiedIDChan, nil
}
