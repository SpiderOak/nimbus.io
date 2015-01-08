package tools

import (
	"testing"
)

const (
	testRedisKey = "TestRedisConnection"
)

func TestRedisConnection(t *testing.T) {
	rawInfo, err := RedisDo("INFO")
	if err != nil {
		t.Fatalf("SET failed %s", err)
	}
	info := rawInfo.([]byte)
	t.Logf("%s", string(info))

	_, err = RedisDo("SETNX", testRedisKey, "glort")
	if err != nil {
		t.Fatalf("SET failed %s", err)
	}

	_, err = RedisDo("DEL", testRedisKey)
	if err != nil {
		t.Fatalf("DEL failed %s", err)
	}
}
