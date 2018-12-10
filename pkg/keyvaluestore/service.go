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

type Service interface {
	io.Closer

	Set(ctx context.Context, request *SetRequest) error
	Get(ctx context.Context, request *GetRequest) (*GetResponse, error)
	Delete(ctx context.Context, request *DeleteRequest) error
}
