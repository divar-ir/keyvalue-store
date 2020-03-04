package static_test

import (
	"testing"

	"github.com/cafebazaar/keyvalue-store/internal/cluster/static"
	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"
	"github.com/stretchr/testify/suite"
)

type StaticClusterTestSuite struct {
	suite.Suite

	local keyvaluestore.Backend
	node1 keyvaluestore.Backend
	node2 keyvaluestore.Backend
	node3 keyvaluestore.Backend
	node4 keyvaluestore.Backend
}

func TestStaticClusterTestSuite(t *testing.T) {
	suite.Run(t, new(StaticClusterTestSuite))
}

func (s *StaticClusterTestSuite) TestReadVoteShouldReturnNumberOfBackendsForConsistencyLevelAll() {
	view, err := s.makeCluster(3, false).Read("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(3, view.VoteRequired)
}

func (s *StaticClusterTestSuite) TestReadVoteShouldReturnMajorityWithEvenBackends() {
	view, err := s.makeCluster(3, false).Read("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(2, view.VoteRequired)
}

func (s *StaticClusterTestSuite) TestReadVoteShouldReturnMajorityWithOddBackends() {
	view, err := s.makeCluster(4, false).Read("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(3, view.VoteRequired)
}

func (s *StaticClusterTestSuite) TestReadVoteShouldReturnOneForConsistencyOne() {
	view, err := s.makeCluster(3, false).Read("", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(1, view.VoteRequired)
}

func (s *StaticClusterTestSuite) TestReadVoteShouldCountLocalConnection() {
	view, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(3, view.VoteRequired)
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldReturnNodeCountForConsistencyAll() {
	view, err := s.makeCluster(3, false).Write("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(3, view.AcknowledgeRequired)
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldReturnMajorityWithEvenBackends() {
	view, err := s.makeCluster(3, false).Write("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(2, view.AcknowledgeRequired)
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldReturnMajorityWithOddBackends() {
	view, err := s.makeCluster(4, false).Write("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(3, view.AcknowledgeRequired)
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldReturnOneWithConsistencyOne() {
	view, err := s.makeCluster(3, false).Write("", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(1, view.AcknowledgeRequired)
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldCountLocalConnection() {
	view, err := s.makeCluster(2, true).Write("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(2, view.AcknowledgeRequired)
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldReturnAllNodesInConsistencyAll() {
	view, err := s.makeCluster(3, false).Write("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(3, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldReturnAllNodesInConsistencyMajority() {
	view, err := s.makeCluster(3, false).Write("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(3, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldReturnAllNodesInConsistencyOne() {
	view, err := s.makeCluster(3, false).Write("", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(3, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldCountLocalConnection() {
	view, err := s.makeCluster(2, true).Write("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(2, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.local})
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldRandomizeNodes() {
	b := s.makeCluster(3, false)
	firstView, err := b.Write("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	found := false
	for i := 0; i < 20; i++ {
		secondView, err := b.Write("", keyvaluestore.ConsistencyLevel_ALL)
		s.Nil(err)
		if firstView.Backends[0] != secondView.Backends[0] {
			found = true
			break
		}
	}
	s.True(found)
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldReturnAllNodesInConsistencyAll() {
	view, err := s.makeCluster(3, false).Read("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(3, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestFlushDbShouldReturnAllBackends() {
	view, err := s.makeCluster(3, false).FlushDB()
	s.Nil(err)
	s.Equal(3, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldReturnAllNodesInConsistencyMajority() {
	view, err := s.makeCluster(3, false).Read("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(3, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldReturnOneNodeForConsistencyOne() {
	view, err := s.makeCluster(3, false).Read("", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(1, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldConsiderLocalConnection() {
	view, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(3, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.local})
}

func (s *StaticClusterTestSuite) TestReadOneFirstAvailablePolicyShouldReturnAllNodesInRead() {
	view, err := s.makeCluster(3, true,
		static.WithPolicy(keyvaluestore.PolicyReadOneFirstAvailable)).Read(
		"", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(3, len(view.Backends))
	s.Subset(view.Backends, []keyvaluestore.Backend{s.node1, s.node2, s.local})
}

func (s *StaticClusterTestSuite) TestReadOneFirstAvailablePolicyShouldLeaveConsistencyAllUnChanged() {
	defaultView, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	policyView, err := s.makeCluster(3, true,
		static.WithPolicy(keyvaluestore.PolicyReadOneFirstAvailable)).Read(
		"", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(defaultView.VotingMode, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadOneLocalOrRandomNodePolicyShouldLeaveConsistencyAllUnChanged() {
	defaultView, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	policyView, err := s.makeCluster(3, true,
		static.WithPolicy(keyvaluestore.PolicyReadOneLocalOrRandomNode)).Read(
		"", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(defaultView.VotingMode, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadOneFirstAvailablePolicyShouldLeaveConsistencyMajorityUnChanged() {
	defaultView, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	policyView, err := s.makeCluster(3, true,
		static.WithPolicy(keyvaluestore.PolicyReadOneFirstAvailable)).Read(
		"", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(defaultView.VotingMode, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadOneLocalOrRandomNodePolicyShouldLeaveConsistencyMajorityUnChanged() {
	defaultView, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	policyView, err := s.makeCluster(3, true,
		static.WithPolicy(keyvaluestore.PolicyReadOneLocalOrRandomNode)).Read(
		"", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(defaultView.VotingMode, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadOneFirstAvailablePolicyShouldApplyToConsistencyOne() {
	policyView, err := s.makeCluster(3, true,
		static.WithPolicy(keyvaluestore.PolicyReadOneFirstAvailable)).Read(
		"", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(keyvaluestore.VotingModeSkipVoteOnNotFound, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadOneLocalOrRandomNodePolicyShouldApplyToConsistencyOne() {
	policyView, err := s.makeCluster(3, true,
		static.WithPolicy(keyvaluestore.PolicyReadOneLocalOrRandomNode)).Read(
		"", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(keyvaluestore.VotingModeVoteOnNotFound, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadOneConsistencyAllShouldHaveDefaultVotingMode() {
	policyView, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	s.Equal(keyvaluestore.VotingModeVoteOnNotFound, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadOneConsistencyMajorityShouldHaveDefaultVotingMode() {
	policyView, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Nil(err)
	s.Equal(keyvaluestore.VotingModeVoteOnNotFound, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadOneConsistencyOneShouldHaveDefaultVotingMode() {
	policyView, err := s.makeCluster(3, true).Read("", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(keyvaluestore.VotingModeVoteOnNotFound, policyView.VotingMode)
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldRandomizeNodes() {
	b := s.makeCluster(3, false)
	firstView, err := b.Read("", keyvaluestore.ConsistencyLevel_ALL)
	s.Nil(err)
	found := false
	for i := 0; i < 20; i++ {
		secondView, err := b.Read("", keyvaluestore.ConsistencyLevel_ALL)
		s.Nil(err)
		if firstView.Backends[0] != secondView.Backends[0] {
			found = true
			break
		}
	}
	s.True(found)
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldReturnOnlyLocalConnection() {
	view, err := s.makeCluster(2, false).Read("", keyvaluestore.ConsistencyLevel_ONE)
	s.Nil(err)
	s.Equal(1, len(view.Backends))
	s.Equal(s.local, view.Backends[0])
}

func (s *StaticClusterTestSuite) TestCloseShouldCloseAllBackends() {
	node := &keyvaluestore.Mock_Backend{}
	node.On("Close").Once().Return(nil)
	cluster := static.New([]keyvaluestore.Backend{node})
	err := cluster.Close()
	s.Nil(err)
	node.AssertExpectations(s.T())
}

func (s *StaticClusterTestSuite) TestCloseShouldCloseLocalConnectionIfProvided() {
	node := &keyvaluestore.Mock_Backend{}
	node.On("Close").Once().Return(nil)
	cluster := static.New(nil, static.WithLocal(node))
	err := cluster.Close()
	s.Nil(err)
	node.AssertExpectations(s.T())
}

func (s *StaticClusterTestSuite) makeCluster(nodes int, local bool,
	clusterOptions ...static.Option) keyvaluestore.Cluster {

	var options []static.Option

	if local {
		options = append(options, static.WithLocal(s.local))
	}

	options = append(options, clusterOptions...)

	all := []keyvaluestore.Backend{
		s.node1,
		s.node2,
		s.node3,
		s.node4,
	}

	return static.New(all[:nodes], options...)
}

func (s *StaticClusterTestSuite) SetupTest() {
	s.local = &keyvaluestore.Mock_Backend{}
	s.node1 = &keyvaluestore.Mock_Backend{}
	s.node2 = &keyvaluestore.Mock_Backend{}
	s.node3 = &keyvaluestore.Mock_Backend{}
	s.node4 = &keyvaluestore.Mock_Backend{}
}
