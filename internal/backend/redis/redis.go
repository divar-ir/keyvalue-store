package redis

import (
	"context"
	"sync"
	"time"

	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"

	"github.com/go-redis/redis"
)

type redisBackend struct {
	client  *redis.Client
	address string
	mutex   sync.Mutex
}

func New(client *redis.Client, address string) keyvaluestore.Backend {
	return &redisBackend{
		client:  client,
		address: address,
	}
}

func (r *redisBackend) Address() string {
	return r.address
}

func (r *redisBackend) Set(key string, value []byte, expiration time.Duration) error {
	client := r.tryGetClient()
	if client == nil {
		return keyvaluestore.ErrClosed
	}

	return client.Set(key, value, expiration).Err()
}

func (r *redisBackend) Lock(ctx context.Context, key string, expiration time.Duration) error {
	var err error
	var ok bool

	ok, err = r.tryLock(key, expiration)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	retryTicker := time.NewTicker(100 * time.Millisecond)
	defer retryTicker.Stop()

	for {
		select {
		case <-retryTicker.C:
			ok, err = r.tryLock(key, expiration)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			continue

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *redisBackend) tryLock(key string, expiration time.Duration) (bool, error) {
	client := r.tryGetClient()
	if client == nil {
		return false, keyvaluestore.ErrClosed
	}

	return client.SetNX(key, "-", expiration).Result()
}

func (r *redisBackend) Unlock(key string) error {
	client := r.tryGetClient()
	if client == nil {
		return keyvaluestore.ErrClosed
	}

	return client.Del(key).Err()
}

func (r *redisBackend) TTL(key string) (*time.Duration, error) {
	client := r.tryGetClient()
	if client == nil {
		return nil, keyvaluestore.ErrClosed
	}

	result, err := client.TTL(key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, keyvaluestore.ErrNotFound
		}

		return nil, err
	}

	if result < 0 {
		return nil, nil
	}

	return &result, nil
}

func (r *redisBackend) Get(key string) ([]byte, error) {
	client := r.tryGetClient()
	if client == nil {
		return nil, keyvaluestore.ErrClosed
	}

	result, err := client.Get(key).Bytes()
	if err == redis.Nil {
		return nil, keyvaluestore.ErrNotFound
	}

	return result, err
}

func (r *redisBackend) Delete(key string) error {
	client := r.tryGetClient()
	if client == nil {
		return keyvaluestore.ErrClosed
	}

	return client.Del(key).Err()
}

func (r *redisBackend) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.client != nil {
		err := r.client.Close()
		r.client = nil

		return err
	}

	return nil
}

func (r *redisBackend) tryGetClient() *redis.Client {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.client
}
