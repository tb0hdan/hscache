package hscache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultName       = "hscache"
	DefaultSleep      = 60 * time.Second
	DefaultFetchCount = 100
)

var (
	ErrKeyExpired = errors.New("key expired")
)

type RedisCompatible interface {
	HGet(ctx context.Context, name string, key string) *redis.StringCmd
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HScan(background context.Context, name string, cursor uint64, s string, count int64) *redis.ScanCmd
}

type Container struct {
	ExpiresTS int64       `json:"expiresTs"`
	Value     interface{} `json:"value"`
}

type HSCache struct {
	client     RedisCompatible
	name       string
	sleep      time.Duration
	fetchCount int64
}

func (c *HSCache) Get(ctx context.Context, key string) (interface{}, error) {
	var (
		container Container
	)
	data, err := c.client.HGet(ctx, c.name, key).Result()
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(data), &container)
	if err != nil {
		return nil, err
	}

	if container.ExpiresTS < time.Now().Unix() {
		// Remove expired key
		if _, err := c.client.HDel(ctx, c.name, key).Result(); err != nil {
			return nil, err
		}
		return nil, ErrKeyExpired
	}

	return container.Value, nil
}
func (c *HSCache) SetEx(ctx context.Context, key string, value interface{}, expiresSeconds int64) error {
	container := Container{
		ExpiresTS: time.Now().Unix() + expiresSeconds,
		Value:     value,
	}

	data, err := json.Marshal(container)
	if err != nil {
		return err
	}

	return c.client.HSet(ctx, c.name, key, string(data)).Err()
}

func (c *HSCache) SetSleep(duration time.Duration) {
	c.sleep = duration
}

func (c *HSCache) SetFetchCount(count int64) {
	c.fetchCount = count
}

func (c *HSCache) Evictor() {
	var (
		container Container
	)
	cursor := uint64(0)
	for {
		keys, newCursor, err := c.client.HScan(context.Background(), c.name, cursor, "*", c.fetchCount).Result()
		if err != nil {
			time.Sleep(c.sleep)
			continue
		}
		if newCursor == 0 {
			time.Sleep(c.sleep)
			continue
		}
		for _, key := range keys {
			data, err := c.client.HGet(context.Background(), c.name, key).Result()
			if err != nil {
				continue
			}

			err = json.Unmarshal([]byte(data), &container)
			if err != nil {
				continue
			}

			if container.ExpiresTS < time.Now().Unix() {
				// Remove expired key
				if _, err := c.client.HDel(context.Background(), c.name, key).Result(); err != nil {
					continue
				}
			}
		}
		cursor = newCursor
	}
}

func New(client RedisCompatible, name string) *HSCache {
	if name == "" {
		name = DefaultName
	}
	return &HSCache{
		client:     client,
		name:       name,
		sleep:      DefaultSleep,
		fetchCount: DefaultFetchCount,
	}
}
