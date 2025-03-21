package services

import (
	"context"
	"fmt"
	"medical-gas-transport-service/config"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	Rdb *redis.Client
}

func NewRedisClient(conf config.RedisConfig) (*Redis, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     conf.URL,
		Password: conf.Password,
		Username: conf.Username,
		DB:       conf.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Health check by pinging the Redis server
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Redis: " + err.Error())
	}

	return &Redis{
		Rdb: rdb,
	}, nil
}
