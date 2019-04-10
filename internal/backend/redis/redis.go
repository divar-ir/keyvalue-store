package redis

import (
	"time"

	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"

	"github.com/go-redis/redis"
)

type redisBackend struct {
	client  *redis.Client
	address string
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
	if r.client == nil {
		return keyvaluestore.ErrClosed
	}

	return r.client.Set(key, value, expiration).Err()
}

func (r *redisBackend) Lock(key string, expiration time.Duration) error {
	if r.client == nil {
		return keyvaluestore.ErrClosed
	}

	ok, err := r.client.SetNX(key, "-", expiration).Result()
	if err != nil {
		return err
	}
	if !ok {
		return keyvaluestore.ErrNotAcquired
	}
	return nil
}

func (r *redisBackend) Unlock(key string) error {
	if r.client == nil {
		return keyvaluestore.ErrClosed
	}

	return r.client.Del(key).Err()
}

func (r *redisBackend) TTL(key string) (*time.Duration, error) {
	if r.client == nil {
		return nil, keyvaluestore.ErrClosed
	}

	result, err := r.client.TTL(key).Result()
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

func (r *redisBackend) Exists(key string) (bool, error) {
	if r.client == nil {
		return false, keyvaluestore.ErrClosed
	}

	result, err := r.client.Exists(key).Result()
	if err != nil {
		return false, err
	}

	return result > 0, nil
}

func (r *redisBackend) Get(key string) ([]byte, error) {
	if r.client == nil {
		return nil, keyvaluestore.ErrClosed
	}

	result, err := r.client.Get(key).Bytes()
	if err == redis.Nil {
		return nil, keyvaluestore.ErrNotFound
	}

	return result, err
}

func (r *redisBackend) Delete(key string) error {
	if r.client == nil {
		return keyvaluestore.ErrClosed
	}

	return r.client.Del(key).Err()
}

func (r *redisBackend) Close() error {
	if r.client != nil {
		err := r.client.Close()
		r.client = nil

		return err
	}

	return nil
}
