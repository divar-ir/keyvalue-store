package keyvaluestore

import (
	"context"
	"io"
	"time"
)

type ConsistencyLevel int

const (
	ConsistencyLevel_DEFAULT  ConsistencyLevel = 0
	ConsistencyLevel_ONE      ConsistencyLevel = 1
	ConsistencyLevel_MAJORITY ConsistencyLevel = 2
	ConsistencyLevel_ALL      ConsistencyLevel = 3
)

type SetRequest struct {
	Key        string
	Data       []byte
	Expiration time.Duration
	Options    WriteOptions
}

type LockRequest struct {
	Key        string
	Data       []byte
	Expiration time.Duration
	Options    WriteOptions
}

type UnlockRequest struct {
	Key     string
	Options WriteOptions
}

type GetRequest struct {
	Key     string
	Options ReadOptions
}

type GetResponse struct {
	Data []byte
}

type DeleteRequest struct {
	Key     string
	Options WriteOptions
}

type WriteOptions struct {
	Consistency ConsistencyLevel
}

type ReadOptions struct {
	Consistency ConsistencyLevel
}

type ExistsRequest struct {
	Key     string
	Options ReadOptions
}

type ExistsResponse struct {
	Exists bool
}

type GetTTLRequest struct {
	Key     string
	Options ReadOptions
}

type GetTTLResponse struct {
	TTL *time.Duration
}

type ExpireRequest struct {
	Key        string
	Expiration time.Duration
	Options    WriteOptions
}

type ExpireResponse struct {
	Exists bool
}

type Service interface {
	io.Closer

	Set(ctx context.Context, request *SetRequest) error
	Get(ctx context.Context, request *GetRequest) (*GetResponse, error)
	Delete(ctx context.Context, request *DeleteRequest) error
	Lock(ctx context.Context, request *LockRequest) error
	Unlock(ctx context.Context, request *UnlockRequest) error
	Exists(ctx context.Context, request *ExistsRequest) (*ExistsResponse, error)
	GetTTL(ctx context.Context, request *GetTTLRequest) (*GetTTLResponse, error)
	Expire(ctx context.Context, request *ExpireRequest) (*ExpireResponse, error)
	FlushDB(ctx context.Context) error
}
