package static

import (
	"fmt"
	"math/rand"

	"github.com/sirupsen/logrus"

	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"
)

type staticCluster struct {
	local    keyvaluestore.Backend
	backends []keyvaluestore.Backend
}

type Option func(s *staticCluster)

func WithLocal(local keyvaluestore.Backend) Option {
	return func(s *staticCluster) {
		s.local = local
	}
}

func New(backends []keyvaluestore.Backend, options ...Option) keyvaluestore.Cluster {
	result := staticCluster{
		backends: backends,
	}

	for _, option := range options {
		option(&result)
	}

	return result
}

func (s staticCluster) WriteAcknowledgeRequired(key string, consistency keyvaluestore.ConsistencyLevel) int {
	switch consistency {
	case keyvaluestore.ConsistencyLevel_ALL:
		return len(s.allNodes())

	case keyvaluestore.ConsistencyLevel_MAJORITY:
		return s.majority(len(s.allNodes()))

	case keyvaluestore.ConsistencyLevel_ONE:
		return 1

	default:
		panic(fmt.Sprintf("unknown consistency level: %v", consistency))
	}
}

func (s staticCluster) ReadVoteRequired(key string, consistency keyvaluestore.ConsistencyLevel) int {
	switch consistency {
	case keyvaluestore.ConsistencyLevel_ALL:
		return len(s.allNodes())

	case keyvaluestore.ConsistencyLevel_MAJORITY:
		return s.majority(len(s.allNodes()))

	case keyvaluestore.ConsistencyLevel_ONE:
		return 1

	default:
		panic(fmt.Sprintf("unknown consistency level: %v", consistency))
	}
}

func (s staticCluster) ReadBackends(key string, consistency keyvaluestore.ConsistencyLevel) []keyvaluestore.Backend {
	switch consistency {
	case keyvaluestore.ConsistencyLevel_ALL:
		return s.allNodes()

	case keyvaluestore.ConsistencyLevel_MAJORITY:
		return s.allNodes()

	case keyvaluestore.ConsistencyLevel_ONE:
		return s.allNodes()[:1]

	default:
		panic(fmt.Sprintf("unknown consistency level: %v", consistency))
	}
}

func (s staticCluster) WriteBackends(key string, consistency keyvaluestore.ConsistencyLevel) []keyvaluestore.Backend {
	switch consistency {
	case keyvaluestore.ConsistencyLevel_ALL:
		return s.allNodes()

	case keyvaluestore.ConsistencyLevel_MAJORITY:
		return s.allNodes()

	case keyvaluestore.ConsistencyLevel_ONE:
		return s.allNodes()

	default:
		panic(fmt.Sprintf("unknown consistency level: %v", consistency))
	}
}

func (s staticCluster) Close() error {
	var lastErr error

	if s.local != nil {
		lastErr = s.local.Close()
	}

	for _, backend := range s.backends {
		if err := backend.Close(); err != nil {
			if lastErr != nil {
				logrus.WithError(err).Error("unexpected error while closing backends")
			}

			lastErr = err
		}
	}

	return lastErr
}

func (s staticCluster) allNodes() []keyvaluestore.Backend {
	backends := s.randomize(s.backends)

	if s.local != nil {
		return append([]keyvaluestore.Backend{s.local}, backends...)
	}

	return backends
}

func (s staticCluster) randomize(backends []keyvaluestore.Backend) []keyvaluestore.Backend {
	result := append([]keyvaluestore.Backend{}, backends...)

	for i := 0; i < len(result); i++ {
		j := i + rand.Intn(len(result)-i)
		temp := result[i]
		result[i] = result[j]
		result[j] = temp
	}

	return result
}

func (s staticCluster) majority(count int) int {
	return (count / 2) + 1
}
