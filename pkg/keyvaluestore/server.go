package keyvaluestore

import (
	"io"
)

type Server interface {
	io.Closer

	Start() error
}
