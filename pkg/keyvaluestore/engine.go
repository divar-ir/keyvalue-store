package keyvaluestore

import (
	"io"
)

type ReadOperator func(backend Backend) (interface{}, error)
type WriteOperator func(backend Backend) error
type RepairOperator func(args RepairArgs)

type RepairArgs struct {
	Value   interface{}
	Err     error
	Winners []Backend
	Losers  []Backend
}

type Engine interface {
	io.Closer

	Read(nodes []Backend, votesRequired int,
		operator ReadOperator, repair RepairOperator, cmp ValueComparer) (interface{}, error)

	Write(nodes []Backend, acknowledgeRequired int,
		operator WriteOperator) error
}
