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
	s.Equal(3, s.makeCluster(3, false).ReadVoteRequired("", keyvaluestore.ConsistencyLevel_ALL))
}

func (s *StaticClusterTestSuite) TestReadVoteShouldReturnMajorityWithEvenBackends() {
	s.Equal(2, s.makeCluster(3, false).ReadVoteRequired("", keyvaluestore.ConsistencyLevel_MAJORITY))
}

func (s *StaticClusterTestSuite) TestReadVoteShouldReturnMajorityWithOddBackends() {
	s.Equal(3, s.makeCluster(4, false).ReadVoteRequired("", keyvaluestore.ConsistencyLevel_MAJORITY))
}

func (s *StaticClusterTestSuite) TestReadVoteShouldReturnOneForConsistencyOne() {
	s.Equal(1, s.makeCluster(3, false).ReadVoteRequired("", keyvaluestore.ConsistencyLevel_ONE))
}

func (s *StaticClusterTestSuite) TestReadVoteShouldCountLocalConnection() {
	s.Equal(3, s.makeCluster(2, true).ReadVoteRequired("", keyvaluestore.ConsistencyLevel_ALL))
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldReturnNodeCountForConsistencyAll() {
	s.Equal(3, s.makeCluster(3, false).WriteAcknowledgeRequired("", keyvaluestore.ConsistencyLevel_ALL))
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldReturnMajorityWithEvenBackends() {
	s.Equal(2, s.makeCluster(3, false).WriteAcknowledgeRequired("", keyvaluestore.ConsistencyLevel_MAJORITY))
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldReturnMajorityWithOddBackends() {
	s.Equal(3, s.makeCluster(4, false).WriteAcknowledgeRequired("", keyvaluestore.ConsistencyLevel_MAJORITY))
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldReturnOneWithConsistencyOne() {
	s.Equal(1, s.makeCluster(3, false).WriteAcknowledgeRequired("", keyvaluestore.ConsistencyLevel_ONE))
}

func (s *StaticClusterTestSuite) TestWriteAcknowledgeShouldCountLocalConnection() {
	s.Equal(3, s.makeCluster(2, true).WriteAcknowledgeRequired("", keyvaluestore.ConsistencyLevel_ALL))
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldReturnAllNodesInConsistencyAll() {
	nodes := s.makeCluster(3, false).WriteBackends("", keyvaluestore.ConsistencyLevel_ALL)
	s.Equal(3, len(nodes))
	s.Subset(nodes, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldReturnAllNodesInConsistencyMajority() {
	nodes := s.makeCluster(3, false).WriteBackends("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Equal(3, len(nodes))
	s.Subset([]keyvaluestore.Backend{s.node1, s.node2, s.node3}, nodes)
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldReturnAllNodesInConsistencyOne() {
	nodes := s.makeCluster(3, false).WriteBackends("", keyvaluestore.ConsistencyLevel_ONE)
	s.Equal(3, len(nodes))
	s.Subset([]keyvaluestore.Backend{s.node1, s.node2, s.node3}, nodes)
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldCountLocalConnection() {
	nodes := s.makeCluster(2, true).WriteBackends("", keyvaluestore.ConsistencyLevel_ALL)
	s.Equal(3, len(nodes))
	s.Subset(nodes, []keyvaluestore.Backend{s.node1, s.node2, s.local})
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldPriotorizeLocalConnection() {
	nodes := s.makeCluster(2, true).WriteBackends("", keyvaluestore.ConsistencyLevel_ALL)
	s.Equal(s.local, nodes[0])
}

func (s *StaticClusterTestSuite) TestWriteBackendsShouldRandomizeNodes() {
	b := s.makeCluster(3, false)
	first := b.WriteBackends("", keyvaluestore.ConsistencyLevel_ALL)[0]
	found := false
	for i := 0; i < 20; i++ {
		second := b.WriteBackends("", keyvaluestore.ConsistencyLevel_ALL)[0]
		if first != second {
			found = true
			break
		}
	}
	s.True(found)
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldReturnAllNodesInConsistencyAll() {
	nodes := s.makeCluster(3, false).ReadBackends("", keyvaluestore.ConsistencyLevel_ALL)
	s.Equal(3, len(nodes))
	s.Subset(nodes, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldReturnAllNodesInConsistencyMajority() {
	nodes := s.makeCluster(3, false).ReadBackends("", keyvaluestore.ConsistencyLevel_MAJORITY)
	s.Equal(3, len(nodes))
	s.Subset(nodes, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldReturnOneNodeForConsistencyOne() {
	nodes := s.makeCluster(3, false).ReadBackends("", keyvaluestore.ConsistencyLevel_ONE)
	s.Equal(1, len(nodes))
	s.Subset(nodes, []keyvaluestore.Backend{s.node1, s.node2, s.node3})
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldConsiderLocalConnection() {
	nodes := s.makeCluster(2, true).ReadBackends("", keyvaluestore.ConsistencyLevel_ALL)
	s.Equal(3, len(nodes))
	s.Subset(nodes, []keyvaluestore.Backend{s.node1, s.node2, s.local})
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldRandomizeNodes() {
	b := s.makeCluster(3, false)
	first := b.ReadBackends("", keyvaluestore.ConsistencyLevel_ALL)[0]
	found := false
	for i := 0; i < 20; i++ {
		second := b.ReadBackends("", keyvaluestore.ConsistencyLevel_ALL)[0]
		if first != second {
			found = true
			break
		}
	}
	s.True(found)
}

func (s *StaticClusterTestSuite) TestReadBackendsShouldReturnOnlyLocalConnection() {
	nodes := s.makeCluster(2, false).ReadBackends("", keyvaluestore.ConsistencyLevel_ONE)
	s.Equal(1, len(nodes))
	s.Equal(s.local, nodes[0])
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

func (s *StaticClusterTestSuite) makeCluster(nodes int, local bool) keyvaluestore.Cluster {
	var options []static.Option

	if local {
		options = append(options, static.WithLocal(s.local))
	}

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
