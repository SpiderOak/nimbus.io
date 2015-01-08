package tools

import (
	"fmt"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"

	"fog"
)

const (
	redisRetryCount = 3
)

var (
	redisAddress string
)

func init() {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}
	redisAddress = fmt.Sprintf("%s:%s", redisHost, redisPort)
}

func RedisDo(commandName string, args ...interface{}) (interface{}, error) {

	var redisConn redis.Conn
	var err error

	for i := 0; i < redisRetryCount; i++ {
		if redisConn, err = redis.Dial("tcp", redisAddress); err != nil {
			fog.Warn("redis.Dial: %s", err)
			time.Sleep(5 * time.Second)
			continue
		}

		result, err := redisConn.Do(commandName, args...)
		redisConn.Close()
		if err != nil {
			fog.Warn("RedisDo: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		return result, nil
	}

	return nil, fmt.Errorf("RedisDo: failed after %d retries %s",
		redisRetryCount, err)
}
