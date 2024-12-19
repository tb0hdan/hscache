package hscache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultName       = "hscache"
	DefaultSleep      = 60 * time.Second
	DefaultFetchCount = 100
)

type Cache interface {
}

type Container struct {
	ExpiresTS int64 `json:"expires_ts"`
	Value     interface{}
}

type HSCache struct {
	client     *redis.Client
	name       string
	sleep      time.Duration
	fetchCount int64
}

func (c *HSCache) Get(ctx context.Context, key string) (interface{}, error) {
	data, err := c.client.HGet(ctx, c.name, key).Result()
	if err != nil {
		return nil, err
	}

	container := Container{}
	err = json.Unmarshal([]byte(data), &container)
	if err != nil {
		return nil, err
	}

	if container.ExpiresTS < time.Now().Unix() {
		// Remove expired key
		if _, err := c.client.HDel(ctx, c.name, key).Result(); err != nil {
			return nil, err
		}
		return nil, nil
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

			container := Container{}
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

func New(client *redis.Client, name string) *HSCache {
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
