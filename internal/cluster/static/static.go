package static

import (
	"math/rand"

	"github.com/pkg/errors"
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

func (s staticCluster) Read(key string,
	consistency keyvaluestore.ConsistencyLevel) (keyvaluestore.ReadClusterView, error) {

	switch consistency {
	case keyvaluestore.ConsistencyLevel_ALL:
		allNodes := s.allNodes()
		return keyvaluestore.ReadClusterView{
			Backends:     allNodes,
			VoteRequired: len(allNodes),
		}, nil

	case keyvaluestore.ConsistencyLevel_MAJORITY:
		allNodes := s.allNodes()
		return keyvaluestore.ReadClusterView{
			Backends:     allNodes,
			VoteRequired: s.majority(len(allNodes)),
		}, nil

	case keyvaluestore.ConsistencyLevel_ONE:
		return keyvaluestore.ReadClusterView{
			Backends:     s.localNodeOrRandomNode(),
			VoteRequired: 1,
		}, nil

	default:
		return keyvaluestore.ReadClusterView{}, errors.Errorf("unknown consistency level: %v", consistency)
	}
}

func (s staticCluster) Write(key string,
	consistency keyvaluestore.ConsistencyLevel) (keyvaluestore.WriteClusterView, error) {

	allNodes := s.allNodes()

	switch consistency {
	case keyvaluestore.ConsistencyLevel_ALL:
		return keyvaluestore.WriteClusterView{
			Backends:            allNodes,
			AcknowledgeRequired: len(allNodes),
		}, nil

	case keyvaluestore.ConsistencyLevel_MAJORITY:
		return keyvaluestore.WriteClusterView{
			Backends:            allNodes,
			AcknowledgeRequired: s.majority(len(allNodes)),
		}, nil

	case keyvaluestore.ConsistencyLevel_ONE:
		return keyvaluestore.WriteClusterView{
			Backends:            allNodes,
			AcknowledgeRequired: 1,
		}, nil

	default:
		return keyvaluestore.WriteClusterView{}, errors.Errorf("unknown consistency level: %v", consistency)
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

func (s staticCluster) localNodeOrRandomNode() []keyvaluestore.Backend {
	if s.local != nil {
		return []keyvaluestore.Backend{s.local}
	}

	return s.allNodes()[:1]
}

func (s staticCluster) allNodes() []keyvaluestore.Backend {
	return s.randomize(s.backends)
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
