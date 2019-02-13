package keyvaluestore

import (
	"io"
	"time"
)

type Backend interface {
	io.Closer

	Set(key string, value []byte, expiration time.Duration) error
	TTL(key string) (*time.Duration, error)
	Get(key string) ([]byte, error)
	Delete(key string) error
	Address() string
}
