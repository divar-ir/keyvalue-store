package keyvaluestore

import (
	"io"
)

type Cluster interface {
	io.Closer

	Read(key string, consistency ConsistencyLevel) (ReadClusterView, error)
	Write(key string, consistency ConsistencyLevel) (WriteClusterView, error)
}

type ReadClusterView struct {
	Backends     []Backend
	VoteRequired int
}

type WriteClusterView struct {
	Backends            []Backend
	AcknowledgeRequired int
}
