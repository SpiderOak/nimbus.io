package tools

import (
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"

	"fog"
)

const (
	redisRetryCount = 3
)

var (
	redisAddress string
	redisConn    redis.Conn
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

	var err error

	for i := 0; i < redisRetryCount; i++ {
		if redisConn == nil {
			if redisConn, err = redis.Dial("tcp", redisAddress); err != nil {
				return nil, err
			}
		}

		result, err := redisConn.Do(commandName, args...)
		if err != nil {
			fog.Warn("RedisDo: %s", err)
			redisConn.Close()
			redisConn = nil
			continue
		}

		return result, nil
	}

	return nil, fmt.Errorf("RedisDo: failed after %d retries %s",
		redisRetryCount, err)
}
