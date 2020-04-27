package keyvaluestore

import (
	"context"
	"io"
	"time"
)

type Backend interface {
	io.Closer

	Set(key string, value []byte, expiration time.Duration) error
	Lock(ctx context.Context, key string, expiration time.Duration) error
	Unlock(key string) error
	TTL(key string) (*time.Duration, error)
	Get(key string) ([]byte, error)
	Delete(key string) error
	Address() string
}
