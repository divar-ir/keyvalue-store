package keyvaluestore

import (
	"io"
)

type Cluster interface {
	io.Closer

	ReadBackends(key string, consistency ConsistencyLevel) []Backend
	ReadVoteRequired(key string, consistency ConsistencyLevel) int
	WriteBackends(key string, consistency ConsistencyLevel) []Backend
	WriteAcknowledgeRequired(key string, consistency ConsistencyLevel) int
}
