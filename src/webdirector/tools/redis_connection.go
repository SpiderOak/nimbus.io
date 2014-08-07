package tools

import (
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"
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

func GetRedisConnection() (redis.Conn, error) {
	var err error

	if redisConn == nil {
		redisConn, err = redis.Dial("tcp", redisAddress)
	}

	return redisConn, err
}
