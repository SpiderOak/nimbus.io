package tools

import (
	"testing"
)

const (
	testRedisKey = "TestRedisConnection"
)

func TestRedisConnection(t *testing.T) {
	conn, err := GetRedisConnection()
	if err != nil {
		t.Fatalf("GetRedisConnection failed %s", err)
	}
	defer conn.Close()

	rawInfo, err := conn.Do("INFO")
	if err != nil {
		t.Fatalf("SET failed %s", err)
	}
	info := rawInfo.([]byte)
	t.Logf("%s", string(info))

	_, err = conn.Do("SETNX", testRedisKey, "glort")
	if err != nil {
		t.Fatalf("SET failed %s", err)
	}

	_, err = conn.Do("DEL", testRedisKey)
	if err != nil {
		t.Fatalf("DEL failed %s", err)
	}
}
