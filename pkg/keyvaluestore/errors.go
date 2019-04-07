package keyvaluestore

import (
	"errors"
)

var (
	ErrClosed      = errors.New("closed")
	ErrConsistency = errors.New("consistency not satisfied")
	ErrNotFound    = errors.New("not found")
	ErrNotAcquired = errors.New("lock not acquired")
)
