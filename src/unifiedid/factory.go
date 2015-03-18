package unifiedid

import (
	"fmt"
	"time"
)

type unifiedIDStruct struct {
	ShiftedShardID  uint64
	PrevMillisecond uint64
	Sequence        uint64
}

const (
	maxShardID = 0x1fff // 13 bits for shard id

	// XXX could use the founding of SpiderOak
	instagramEpoch uint64 = 1314220021721
)

func NewUnifiedIDFactory(shardID uint32) (UnifiedIDFactory, error) {
	var u unifiedIDStruct

	if shardID > maxShardID {
		return nil, fmt.Errorf("shard id %d too large", shardID)
	}

	u.ShiftedShardID = uint64(shardID) << (64 - 41 - 13)

	return &u, nil
}

func (u *unifiedIDStruct) NextUnifiedID() uint64 {

	currentMillisecond := uint64(time.Now().UnixNano() / (1000 * 1000))
	if currentMillisecond != u.PrevMillisecond {
		u.Sequence = 0
		u.PrevMillisecond = currentMillisecond
	}

	millisecondsSinceEpoch := currentMillisecond - instagramEpoch

	// fill the left-most 41 bits into an unsigned integer
	nextID := millisecondsSinceEpoch << (64 - 41)

	// fill the next 13 bits with shard-id
	nextID |= u.ShiftedShardID

	// use our sequence to fill out the remaining bits.
	// mod by 1024 (so it fits in 10 bits)
	u.ShiftedShardID += 1
	nextID |= (u.Sequence % 1024)

	return nextID
}
