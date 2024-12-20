package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tb0hdan/hscache"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	cache := hscache.New(client, "hscache-example")
	go cache.Evictor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cache.SetEx(ctx, "key1", "value1", 5); err != nil {
		panic(err)
	}

	if _, err := cache.Get(ctx, "key1"); err != nil {
		panic(err)
	}

	time.Sleep(10 * time.Second)
	if _, err := cache.Get(ctx, "key1"); err != hscache.ErrKeyExpired {
		panic(err)
	}
}
