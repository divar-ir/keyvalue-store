package keyvaluestore

import (
	"io"
)

type Policy int

var (
	PolicyReadOneLocalOrRandomNode Policy
	PolicyReadOneFirstAvailable    Policy = 1
)

type Cluster interface {
	io.Closer

	Read(key string, consistency ConsistencyLevel) (ReadClusterView, error)
	Write(key string, consistency ConsistencyLevel) (WriteClusterView, error)
}

type ReadClusterView struct {
	Backends     []Backend
	VoteRequired int
	VotingMode   VotingMode
}

type WriteClusterView struct {
	Backends            []Backend
	AcknowledgeRequired int
}
