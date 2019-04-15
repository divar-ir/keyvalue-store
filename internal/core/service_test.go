package core_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cafebazaar/keyvalue-store/internal/core"
	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	KEY   = "mykey"
	VALUE = "Hello, World!"
)

var (
	ONE_MINUTE  = 1 * time.Minute
	ZERO_MINUTE = 0 * time.Minute
)

type CoreServiceTestSuite struct {
	suite.Suite

	node1 *keyvaluestore.Mock_Backend
	node2 *keyvaluestore.Mock_Backend
	node3 *keyvaluestore.Mock_Backend
	nodes []keyvaluestore.Backend

	cluster *keyvaluestore.Mock_Cluster
	engine  *keyvaluestore.Mock_Engine
	core    keyvaluestore.Service

	dataStr []byte

	dataStrMatcher func(data interface{}) bool
}

func TestCoreServiceTestSuite(t *testing.T) {
	suite.Run(t, new(CoreServiceTestSuite))
}

func (s *CoreServiceTestSuite) TestSetShouldEncodeStringData() {
	s.node1.On("Set", KEY, mock.MatchedBy(s.dataStrMatcher), mock.Anything).Once().Return(nil)
	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(1)
	err := s.core.Set(context.Background(), &keyvaluestore.SetRequest{
		Data: s.dataStr,
		Key:  KEY,
		Options: keyvaluestore.WriteOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestSetShouldNotUseDefaultWriteConsistencyIfRequestHasProvided() {
	s.applyCore(core.WithDefaultWriteConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(0)
	err := s.core.Set(context.Background(), &keyvaluestore.SetRequest{
		Data: s.dataStr,
		Key:  KEY,
		Options: keyvaluestore.WriteOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestSetShouldUseDefaultWriteConsistencyIfRequestIsEmpty() {
	s.applyCore(core.WithDefaultWriteConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyWriteToEngineOnce(0)
	err := s.core.Set(context.Background(), &keyvaluestore.SetRequest{
		Data: s.dataStr,
		Key:  KEY,
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestSetShouldNotEmployTTLIfRequestHasNotProvided() {
	s.node1.On("Set", KEY, mock.Anything, time.Duration(0)).Return(nil)
	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(1)
	err := s.core.Set(context.Background(), &keyvaluestore.SetRequest{
		Key: KEY,
		Options: keyvaluestore.WriteOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestSetShouldEmployTTLIfRequestHasProvided() {
	s.node1.On("Set", KEY, mock.Anything, 1*time.Minute).Return(nil)
	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(1)
	err := s.core.Set(context.Background(), &keyvaluestore.SetRequest{
		Data:       s.dataStr,
		Key:        KEY,
		Expiration: 1 * time.Minute,
		Options: keyvaluestore.WriteOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestGetShouldCallGetUponBackends() {
	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(s.dataStr, nil, nil, 1,
		keyvaluestore.VotingModeVoteOnNotFound)
	value, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.Equal(VALUE, string(value.Data))
}

func (s *CoreServiceTestSuite) TestGetShouldNotUseDefaultConsistencyLevelIfRequestProvidesIt() {
	s.applyCore(core.WithDefaultReadConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(s.dataStr, nil, nil, 0,
		keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestGetShouldUseDefaultConsistencyLevelIfRequestDoesNotProvidesIt() {
	s.applyCore(core.WithDefaultReadConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyReadToEngineOnce(s.dataStr, nil, nil, 0,
		keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestGetShouldRepairWithDeleteIfResultIsNotFound() {
	s.node1.On("Delete", KEY).Once().Return(nil)
	s.applyCore()
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(0)
	s.applyReadToEngineOnce(s.dataStr, keyvaluestore.ErrNotFound, &keyvaluestore.RepairArgs{
		Err:    keyvaluestore.ErrNotFound,
		Losers: []keyvaluestore.Backend{s.node1},
	}, 0, keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.assertStatusCode(err, codes.NotFound)
	s.node1.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestGetShouldForfeitRepairIfTTLHitsError() {
	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node2.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node3.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.node1.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)
	s.node2.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)
	s.node3.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(s.dataStr, nil, &keyvaluestore.RepairArgs{
		Winners: []keyvaluestore.Backend{s.node1, s.node2, s.node3},
		Value:   s.dataStr,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(time.Duration(0), errors.New("some error"), nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	_, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestGetShouldAcquireTTLAndApplyToLosers() {
	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node2.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node3.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.node1.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)
	s.node2.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)

	s.node3.On("Set", KEY, s.dataStr, time.Duration(1*time.Minute)).Once().Return(nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(s.dataStr, nil, &keyvaluestore.RepairArgs{
		Losers:  []keyvaluestore.Backend{s.node3},
		Winners: []keyvaluestore.Backend{s.node1, s.node2},
		Value:   s.dataStr,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(&ONE_MINUTE, nil, nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	s.applyWriteToEngineOnce(0)

	_, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestGetShouldNotApplyTTLDuringRepairIfItDoesNotExist() {
	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node2.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node3.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.node1.On("TTL", KEY).Once().Return(nil, nil)
	s.node2.On("TTL", KEY).Once().Return(nil, nil)

	s.node3.On("Set", KEY, s.dataStr, time.Duration(0)).Once().Return(nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(s.dataStr, nil, &keyvaluestore.RepairArgs{
		Losers:  []keyvaluestore.Backend{s.node3},
		Winners: []keyvaluestore.Backend{s.node1, s.node2},
		Value:   s.dataStr,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(nil, nil, nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	s.applyWriteToEngineOnce(0)

	_, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestGetShouldForfeitRepairIfTTLIsZero() {
	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node2.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node3.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.node1.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)
	s.node2.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(s.dataStr, nil, &keyvaluestore.RepairArgs{
		Losers:  []keyvaluestore.Backend{s.node3},
		Winners: []keyvaluestore.Backend{s.node1, s.node2},
		Value:   s.dataStr,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(&ZERO_MINUTE, nil, nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)

	_, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestGetShouldUseVotingModeFromClusterView() {
	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL,
		s.withVotingMode(keyvaluestore.VotingModeSkipVoteOnNotFound))
	s.applyReadToEngineOnce(s.dataStr, nil, nil, 1,
		keyvaluestore.VotingModeSkipVoteOnNotFound)

	_, err := s.core.Get(context.Background(), &keyvaluestore.GetRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestGetTTLShouldCallTTLUponBackends() {
	s.node1.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)
	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(&ONE_MINUTE, nil, nil, 1,
		keyvaluestore.VotingModeVoteOnNotFound)
	value, err := s.core.GetTTL(context.Background(), &keyvaluestore.GetTTLRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.NotNil(value.TTL)
	if value.TTL != nil {
		s.Equal(1*time.Minute, *value.TTL)
	}
}

func (s *CoreServiceTestSuite) TestGetTTLShouldNotUseDefaultConsistencyLevelIfRequestProvidesIt() {
	s.applyCore(core.WithDefaultReadConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(&ONE_MINUTE, nil, nil, 0,
		keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.GetTTL(context.Background(), &keyvaluestore.GetTTLRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestGetTTLShouldUseDefaultConsistencyLevelIfRequestDoesNotProvidesIt() {
	s.applyCore(core.WithDefaultReadConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyReadToEngineOnce(&ONE_MINUTE, nil, nil, 0,
		keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.GetTTL(context.Background(), &keyvaluestore.GetTTLRequest{
		Key: KEY,
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestGetTTLShouldRepairWithDeleteIfResultIsNotFound() {
	s.node1.On("Delete", KEY).Once().Return(nil)
	s.applyCore()
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(0)
	s.applyReadToEngineOnce(1*time.Second, keyvaluestore.ErrNotFound, &keyvaluestore.RepairArgs{
		Err:    keyvaluestore.ErrNotFound,
		Losers: []keyvaluestore.Backend{s.node1},
	}, 0, keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.GetTTL(context.Background(), &keyvaluestore.GetTTLRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.assertStatusCode(err, codes.NotFound)
	s.node1.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestGetTTLShouldForfeitRepairIfGetHitsError() {
	s.node1.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)
	s.node2.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)
	s.node3.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)

	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node2.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node3.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(&ONE_MINUTE, nil, &keyvaluestore.RepairArgs{
		Winners: []keyvaluestore.Backend{s.node1, s.node2, s.node3},
		Value:   &ONE_MINUTE,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(nil, errors.New("some error"), nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	_, err := s.core.GetTTL(context.Background(), &keyvaluestore.GetTTLRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestGetTTLShouldAcquireDataAndApplyToLosers() {
	s.node1.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)
	s.node2.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)
	s.node3.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)

	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node2.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.node3.On("Set", KEY, s.dataStr, 1*time.Minute).Once().Return(nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(&ONE_MINUTE, nil, &keyvaluestore.RepairArgs{
		Losers:  []keyvaluestore.Backend{s.node3},
		Winners: []keyvaluestore.Backend{s.node1, s.node2},
		Value:   &ONE_MINUTE,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(s.dataStr, nil, nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	s.applyWriteToEngineOnce(0)

	_, err := s.core.GetTTL(context.Background(), &keyvaluestore.GetTTLRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestExistsShouldCallExistsUponBackends() {
	s.node1.On("Exists", KEY).Once().Return(true, nil)
	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(true, nil, nil, 1,
		keyvaluestore.VotingModeVoteOnNotFound)
	value, err := s.core.Exists(context.Background(), &keyvaluestore.ExistsRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.Equal(true, value.Exists)
}

func (s *CoreServiceTestSuite) TestExistsShouldNotUseDefaultConsistencyLevelIfRequestProvidesIt() {
	s.applyCore(core.WithDefaultReadConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(true, nil, nil, 0,
		keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.Exists(context.Background(), &keyvaluestore.ExistsRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestExistsShouldUseDefaultConsistencyLevelIfRequestDoesNotProvidesIt() {
	s.applyCore(core.WithDefaultReadConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyReadToEngineOnce(true, nil, nil, 0,
		keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.Exists(context.Background(), &keyvaluestore.ExistsRequest{
		Key: KEY,
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestExistsShouldRepairWithDeleteIfResultIsNotFound() {
	s.node1.On("Delete", KEY).Once().Return(nil)
	s.applyCore()
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(0)
	s.applyReadToEngineOnce(false, keyvaluestore.ErrNotFound, &keyvaluestore.RepairArgs{
		Value:  false,
		Losers: []keyvaluestore.Backend{s.node1},
		Err:    keyvaluestore.ErrNotFound,
	}, 0, keyvaluestore.VotingModeVoteOnNotFound)
	_, err := s.core.Exists(context.Background(), &keyvaluestore.ExistsRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestExistsShouldForfeitRepairIfTTLHitsError() {
	s.node1.On("Exists", KEY).Once().Return(true, nil)
	s.node2.On("Exists", KEY).Once().Return(true, nil)
	s.node3.On("Exists", KEY).Once().Return(true, nil)

	s.node1.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)
	s.node2.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)
	s.node3.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(true, nil, &keyvaluestore.RepairArgs{
		Winners: []keyvaluestore.Backend{s.node1, s.node2, s.node3},
		Value:   true,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(&ZERO_MINUTE, errors.New("some error"), nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	_, err := s.core.Exists(context.Background(), &keyvaluestore.ExistsRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestExistsShouldForfeitRepairIfGetHitsError() {
	s.node1.On("Exists", KEY).Once().Return(true, nil)
	s.node2.On("Exists", KEY).Once().Return(true, nil)
	s.node3.On("Exists", KEY).Once().Return(true, nil)

	s.node1.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)
	s.node2.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)
	s.node3.On("TTL", KEY).Once().Return(&ZERO_MINUTE, nil)

	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node2.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node3.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(true, nil, &keyvaluestore.RepairArgs{
		Winners: []keyvaluestore.Backend{s.node1, s.node2, s.node3},
		Value:   true,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(&ONE_MINUTE, nil, nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	s.applyReadToEngineOnce(nil, errors.New("some error"), nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	_, err := s.core.Exists(context.Background(), &keyvaluestore.ExistsRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestExistsShouldAcquireTTLWithDataAndApplyToLosers() {
	s.node1.On("Exists", KEY).Once().Return(true, nil)
	s.node2.On("Exists", KEY).Once().Return(true, nil)
	s.node3.On("Exists", KEY).Once().Return(false, nil)

	s.node1.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)
	s.node2.On("TTL", KEY).Once().Return(&ONE_MINUTE, nil)

	s.node1.On("Get", KEY).Once().Return(s.dataStr, nil)
	s.node2.On("Get", KEY).Once().Return(s.dataStr, nil)

	s.node3.On("Set", KEY, s.dataStr, 1*time.Minute).Once().Return(nil)

	s.applyCore()
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_ALL)
	s.applyReadToEngineOnce(true, nil, &keyvaluestore.RepairArgs{
		Losers:  []keyvaluestore.Backend{s.node3},
		Winners: []keyvaluestore.Backend{s.node1, s.node2},
		Value:   true,
	}, 3, keyvaluestore.VotingModeVoteOnNotFound)
	s.applyReadToEngineOnce(&ONE_MINUTE, nil, nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	s.applyReadToEngineOnce(s.dataStr, nil, nil, 2,
		keyvaluestore.VotingModeSkipVoteOnNotFound)
	s.applyWriteToEngineOnce(0)

	_, err := s.core.Exists(context.Background(), &keyvaluestore.ExistsRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
	s.node2.AssertExpectations(s.T())
	s.node3.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestExistsShouldUseVotingModeFromClusterView() {
	s.node1.On("Exists", KEY).Once().Return(true, nil)

	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL,
		s.withVotingMode(keyvaluestore.VotingModeSkipVoteOnNotFound))
	s.applyReadToEngineOnce(true, nil, nil, 1,
		keyvaluestore.VotingModeSkipVoteOnNotFound)

	_, err := s.core.Exists(context.Background(), &keyvaluestore.ExistsRequest{
		Key: KEY,
		Options: keyvaluestore.ReadOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestDeleteShouldCallDeleteOnNodes() {
	s.node1.On("Delete", KEY).Once().Return(nil)
	s.applyCore()
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(1)
	err := s.core.Delete(context.Background(), &keyvaluestore.DeleteRequest{
		Key: KEY,
		Options: keyvaluestore.WriteOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestDeleteShouldNotUseDefaultWriteConsistencyIfProvidedByRequest() {
	s.applyCore(core.WithDefaultWriteConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_ALL)
	s.applyWriteToEngineOnce(0)
	err := s.core.Delete(context.Background(), &keyvaluestore.DeleteRequest{
		Key: KEY,
		Options: keyvaluestore.WriteOptions{
			Consistency: keyvaluestore.ConsistencyLevel_ALL,
		},
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestDeleteShouldUseDefaultWriteConsistencyIfNotProvidedByRequest() {
	s.applyCore(core.WithDefaultWriteConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(0, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyWriteToEngineOnce(0)
	err := s.core.Delete(context.Background(), &keyvaluestore.DeleteRequest{
		Key: KEY,
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestLockShouldCallLockOnNode() {
	s.applyCore(core.WithDefaultWriteConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyWriteToEngineOnce(1, WithMode(keyvaluestore.OperationModeSequential))
	s.node1.On("Lock", KEY, s.dataStr, mock.Anything).Once().Return(nil)
	err := s.core.Lock(context.Background(), &keyvaluestore.LockRequest{
		Key:  KEY,
		Data: s.dataStr,
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestLockShouldPreserveOrder() {
	s.applyCore(core.WithDefaultWriteConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(3, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyWriteToEngineOnce(3, WithOrdering(s.node3, s.node2, s.node1),
		WithMode(keyvaluestore.OperationModeSequential))
	s.node1.On("Lock", KEY, s.dataStr, mock.Anything).Once().Return(nil)
	s.node2.On("Lock", KEY, s.dataStr, mock.Anything).Once().Return(nil)
	s.node3.On("Lock", KEY, s.dataStr, mock.Anything).Once().Return(nil)
	err := s.core.Lock(context.Background(), &keyvaluestore.LockRequest{
		Key:  KEY,
		Data: s.dataStr,
	})
	s.Nil(err)
}

func (s *CoreServiceTestSuite) TestLockShouldRollbackUsingUnlock() {
	s.applyCore(core.WithDefaultWriteConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyWriteToEngineOnce(1,
		WithMode(keyvaluestore.OperationModeSequential),
		WithRollbackArgs(keyvaluestore.RollbackArgs{
			Nodes: []keyvaluestore.Backend{s.node1},
		}))
	s.applyWriteToEngineOnce(0)
	s.node1.On("Lock", KEY, s.dataStr, mock.Anything).Once().Return(errors.New("some error"))
	s.node1.On("Unlock", KEY).Once().Return(nil)
	err := s.core.Lock(context.Background(), &keyvaluestore.LockRequest{
		Key:  KEY,
		Data: s.dataStr,
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) TestUnlockShouldCallUnlockOnBackends() {
	s.applyCore(core.WithDefaultWriteConsistency(keyvaluestore.ConsistencyLevel_MAJORITY))
	s.applyCluster(1, keyvaluestore.ConsistencyLevel_MAJORITY)
	s.applyWriteToEngineOnce(1)
	s.node1.On("Unlock", KEY).Once().Return(nil)
	err := s.core.Unlock(context.Background(), &keyvaluestore.UnlockRequest{
		Key: KEY,
	})
	s.Nil(err)
	s.node1.AssertExpectations(s.T())
}

func (s *CoreServiceTestSuite) assertStatusCode(err error, c codes.Code) {
	grpcStatus, ok := status.FromError(err)

	switch c {
	case codes.OK:
		if err != nil {
			if ok {
				s.Equal(codes.OK, grpcStatus.Code())
			} else {
				s.Nil(err)
			}
		}

	case codes.Internal:
		if err == nil {
			s.NotNil(err)
		} else if ok {
			s.Equal(codes.Internal, grpcStatus.Code())
		}

	default:
		s.True(ok)
		if ok {
			s.Equal(c, grpcStatus.Code())
		}
	}
}

func (s *CoreServiceTestSuite) applyWriteToEngineOnce(nodeCount int, options ...Option) {
	optionCtx := newOptionContext()
	for _, option := range options {
		option(optionCtx)
	}

	s.engine.On("Write", mock.Anything, nodeCount, mock.Anything, mock.Anything,
		optionCtx.mode).Run(func(args mock.Arguments) {

		backends := args.Get(0).([]keyvaluestore.Backend)

		if optionCtx.ordering != nil {
			s.Equal(len(optionCtx.ordering), len(backends))
			for i := 0; i < len(optionCtx.ordering) && i < len(backends); i++ {
				s.Equal(backends[i], optionCtx.ordering[i])
			}
		}

		operator := args.Get(2).(keyvaluestore.WriteOperator)
		for _, backend := range backends {
			if err := operator(backend); err != nil {
				logrus.WithError(err).Info("error during test")
			}
		}

		rollbackOperator := args.Get(3).(keyvaluestore.RollbackOperator)

		if optionCtx.rollbackArgs != nil {
			rollbackOperator(*optionCtx.rollbackArgs)
		}
	}).Return(nil)
}

func (s *CoreServiceTestSuite) applyReadToEngineOnce(result interface{}, err error,
	repairArgs *keyvaluestore.RepairArgs, nodeCount int,
	mode keyvaluestore.VotingMode) {

	s.engine.On("Read", mock.Anything, nodeCount, mock.Anything, mock.Anything, mock.Anything, mode).Once().
		Run(func(args mock.Arguments) {
			backends := args.Get(0).([]keyvaluestore.Backend)
			readOperator := args.Get(2).(keyvaluestore.ReadOperator)
			repairOperator := args.Get(3).(keyvaluestore.RepairOperator)

			for _, backend := range backends {
				if _, err := readOperator(backend); err != nil {
					logrus.WithError(err).Info("error during test")
				}
			}

			if repairArgs != nil {
				repairOperator(*repairArgs)
			}
		}).Return(result, err)
}

type clusterOptionContext struct {
	readView  keyvaluestore.ReadClusterView
	writeView keyvaluestore.WriteClusterView
}

type clusterOption func(o *clusterOptionContext)

func (s *CoreServiceTestSuite) applyCluster(
	nodes int, consistency keyvaluestore.ConsistencyLevel,
	options ...clusterOption) {

	s.nodes = ([]keyvaluestore.Backend{s.node1, s.node2, s.node3})[:nodes]
	optionContext := clusterOptionContext{
		readView: keyvaluestore.ReadClusterView{
			Backends:     s.nodes,
			VoteRequired: len(s.nodes),
			VotingMode:   keyvaluestore.VotingModeVoteOnNotFound,
		},
		writeView: keyvaluestore.WriteClusterView{
			Backends:            s.nodes,
			AcknowledgeRequired: len(s.nodes),
		},
	}

	for _, option := range options {
		option(&optionContext)
	}

	s.cluster.On("Read", KEY, consistency).Return(optionContext.readView, nil)
	s.cluster.On("Write", KEY, consistency).Return(optionContext.writeView, nil)
}

func (s *CoreServiceTestSuite) withVotingMode(mode keyvaluestore.VotingMode) clusterOption {
	return func(o *clusterOptionContext) {
		o.readView.VotingMode = mode
	}
}

func (s *CoreServiceTestSuite) applyCore(options ...core.Option) {
	s.core = core.New(s.cluster, s.engine, options...)
}

type optionContext struct {
	mode         keyvaluestore.OperationMode
	ordering     []*keyvaluestore.Mock_Backend
	rollbackArgs *keyvaluestore.RollbackArgs
}

type Option func(o *optionContext)

func newOptionContext() *optionContext {
	return &optionContext{
		mode: keyvaluestore.OperationModeConcurrent,
	}
}

func WithMode(mode keyvaluestore.OperationMode) Option {
	return func(o *optionContext) {
		o.mode = mode
	}
}

func WithOrdering(nodes ...*keyvaluestore.Mock_Backend) Option {
	for i, node := range nodes {
		node.On("Address").Return(fmt.Sprintf("host-%d", i))
	}

	return func(o *optionContext) {
		o.ordering = nodes
	}
}

func WithRollbackArgs(args keyvaluestore.RollbackArgs) Option {
	return func(o *optionContext) {
		o.rollbackArgs = &args
	}
}

func (s *CoreServiceTestSuite) SetupTest() {
	s.node1 = &keyvaluestore.Mock_Backend{}
	s.node2 = &keyvaluestore.Mock_Backend{}
	s.node3 = &keyvaluestore.Mock_Backend{}
	s.engine = &keyvaluestore.Mock_Engine{}
	s.cluster = &keyvaluestore.Mock_Cluster{}

	s.dataStr = []byte(VALUE)
	s.dataStrMatcher = func(data interface{}) bool {
		raw, ok := data.([]byte)
		s.True(ok)
		if !ok {
			return false
		}

		s.Equal(VALUE, string(raw))
		return VALUE == string(raw)
	}
}
