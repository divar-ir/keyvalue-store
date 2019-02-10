package engine_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cafebazaar/keyvalue-store/internal/engine"
	"github.com/cafebazaar/keyvalue-store/internal/voting"

	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"
	"github.com/stretchr/testify/suite"
)

const (
	RESULT = 101
)

type EngineTestSuite struct {
	suite.Suite

	node1                keyvaluestore.Backend
	node2                keyvaluestore.Backend
	node3                keyvaluestore.Backend
	nodes                []keyvaluestore.Backend
	mark                 []bool
	err                  []error
	result               []int
	slow                 []bool
	continueSlowOperator chan struct{}
	engine               keyvaluestore.Engine
	writeOperator        keyvaluestore.WriteOperator
	readOperator         keyvaluestore.ReadOperator
	emptyRepairOperator  keyvaluestore.RepairOperator
	comparer             keyvaluestore.ValueComparer
	wg                   sync.WaitGroup
}

func TestEngineTestSuite(t *testing.T) {
	suite.Run(t, new(EngineTestSuite))
}

func (s *EngineTestSuite) TestWriteShouldTryWriteOnAllBackends() {
	s.Nil(s.engine.Write(s.nodes, 3, s.writeOperator))
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestWriteShouldNotWaitOnSlowBackendsIfAcknowledgeAreSatisfied() {
	s.setNodeSlow(0)
	s.Nil(s.engine.Write(s.nodes, 2, s.writeOperator))
	s.False(s.mark[0])
	s.continueSlow()
	s.wg.Wait()
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestWriteShouldIgnoreErrorIfAcknowledgeAreSatisfied() {
	s.setNodeOnError(0, errors.New("some error"))
	s.Nil(s.engine.Write(s.nodes, 2, s.writeOperator))
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestWriteShouldReportErrorIfAcknowledgeAreNotSatisfied() {
	s.setNodeOnError(0, errors.New("some error"))
	s.NotNil(s.engine.Write(s.nodes, 3, s.writeOperator))
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldCallAllNodes() {
	value, err := s.engine.Read(s.nodes, 3, s.readOperator, s.emptyRepairOperator, s.comparer)
	s.Nil(err)
	s.Equal(RESULT, value)
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldNotCallRepairIfAllNodesAggree() {
	value, err := s.engine.Read(s.nodes, 3, s.readOperator, func(args keyvaluestore.RepairArgs) {
		s.FailNow("repair should not have been called since all nodes agree")
	}, s.comparer)
	s.Nil(err)
	s.Equal(RESULT, value)
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldNotWaitOnSlowNodesIfVotesAreSatisfied() {
	s.setNodeSlow(0)
	value, err := s.engine.Read(s.nodes, 2, s.readOperator, s.emptyRepairOperator, s.comparer)
	s.Nil(err)
	s.Equal(RESULT, value)
	s.False(s.mark[0])
	s.continueSlow()
	s.wg.Wait()
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldNotReportErrorIfVotesAreSatisfied() {
	s.setNodeOnError(0, errors.New("some error"))
	value, err := s.engine.Read(s.nodes, 2, s.readOperator, s.emptyRepairOperator, s.comparer)
	s.Nil(err)
	s.Equal(RESULT, value)
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldReportErrorIfVotesAreNotSatisfied() {
	s.setNodeOnError(0, errors.New("some error"))
	_, err := s.engine.Read(s.nodes, 3, s.readOperator, s.emptyRepairOperator, s.comparer)
	s.NotNil(err)
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldReportNotFoundErrorIfVotesAggree() {
	s.setNodeOnError(0, keyvaluestore.ErrNotFound)
	s.setNodeOnError(1, keyvaluestore.ErrNotFound)
	_, err := s.engine.Read(s.nodes, 2, s.readOperator, s.emptyRepairOperator, s.comparer)
	s.Equal(keyvaluestore.ErrNotFound, err)
	s.wg.Wait()
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldNotConsiderErrorfulBackendsInRepair() {
	s.setNodeOnError(0, errors.New("some error"))
	value, err := s.engine.Read(s.nodes, 2, s.readOperator, func(args keyvaluestore.RepairArgs) {
		s.FailNow("unexpected method call, node 0 is faulty and should not trigger a repair action")
	}, s.comparer)
	s.Nil(err)
	s.Equal(RESULT, value)
	s.wg.Wait()
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldConsiderNotFoundErrorInRepair() {
	s.setNodeOnError(0, keyvaluestore.ErrNotFound)
	s.wg.Add(1)
	value, err := s.engine.Read(s.nodes, 2, s.readOperator, func(args keyvaluestore.RepairArgs) {
		defer s.wg.Done()

		s.Nil(args.Err)
		s.Equal(RESULT, args.Value)
		s.Equal(1, len(args.Losers))
		s.Equal(2, len(args.Winners))
		s.Equal(s.node1, args.Losers[0])
		s.Subset(args.Winners, []keyvaluestore.Backend{s.node2, s.node3})
	}, s.comparer)
	s.Nil(err)
	s.Equal(RESULT, value)
	s.wg.Wait()
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldConsiderDifferenetValueInRepair() {
	s.setNodeResult(0, RESULT+1)
	s.wg.Add(1)
	value, err := s.engine.Read(s.nodes, 2, s.readOperator, func(args keyvaluestore.RepairArgs) {
		defer s.wg.Done()

		s.Nil(args.Err)
		s.Equal(RESULT, args.Value)
		s.Equal(1, len(args.Losers))
		s.Equal(2, len(args.Winners))
		s.Equal(s.node1, args.Losers[0])
		s.Subset(args.Winners, []keyvaluestore.Backend{s.node2, s.node3})
	}, s.comparer)
	s.Nil(err)
	s.Equal(RESULT, value)
	s.wg.Wait()
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldRepairNodesWithValueIfMajorityReportNotFound() {
	s.setNodeOnError(0, keyvaluestore.ErrNotFound)
	s.setNodeOnError(1, keyvaluestore.ErrNotFound)
	s.wg.Add(1)
	_, err := s.engine.Read(s.nodes, 2, s.readOperator, func(args keyvaluestore.RepairArgs) {
		defer s.wg.Done()

		s.Equal(keyvaluestore.ErrNotFound, args.Err)
		s.Equal(1, len(args.Losers))
		s.Equal(2, len(args.Winners))
		s.Equal(s.node2, args.Losers[0])
		s.Subset(args.Winners, []keyvaluestore.Backend{s.node1, s.node2})
	}, s.comparer)
	s.Equal(keyvaluestore.ErrNotFound, err)
	s.wg.Wait()
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestWriteShouldImmediatelyReturnIfAcknowledgeCountIsZero() {
	s.setNodeSlow(0)
	s.setNodeSlow(1)
	s.setNodeSlow(2)
	s.Nil(s.engine.Write(s.nodes, 0, s.writeOperator))
	s.continueSlow()
	s.wg.Wait()
	s.assertAllCalled()
}

func (s *EngineTestSuite) TestReadShouldSkipRepairIfNilIsProvided() {
	s.setNodeResult(0, RESULT+1)
	value, err := s.engine.Read(s.nodes, 2, s.readOperator, nil, s.comparer)
	s.Nil(err)
	s.Equal(RESULT, value)
	s.assertAllCalled()
}

func (s *EngineTestSuite) assertAllCalled() {
	if s.engine != nil {
		s.Nil(s.engine.Close())
		s.engine = nil
	}

	s.True(s.mark[0])
	s.True(s.mark[1])
	s.True(s.mark[2])
}

func (s *EngineTestSuite) continueSlow() {
	close(s.continueSlowOperator)
}

func (s *EngineTestSuite) setNodeResult(index int, result int) {
	s.result[index] = result
}

func (s *EngineTestSuite) setNodeOnError(index int, err error) {
	s.err[index] = err
}

func (s *EngineTestSuite) setNodeSlow(index int) {
	s.slow[index] = true
}

func (s *EngineTestSuite) indexOf(backend keyvaluestore.Backend) int {
	switch backend {
	case s.node1:
		return 0

	case s.node2:
		return 1

	case s.node3:
		return 2

	default:
		s.FailNow("unexpected backend", backend)
		return -1
	}
}

func (s *EngineTestSuite) SetupTest() {
	s.node1 = &keyvaluestore.Mock_Backend{}
	s.node2 = &keyvaluestore.Mock_Backend{}
	s.node3 = &keyvaluestore.Mock_Backend{}
	s.nodes = []keyvaluestore.Backend{s.node1, s.node2, s.node3}
	s.mark = []bool{false, false, false}
	s.continueSlowOperator = make(chan struct{})
	s.engine = engine.New(voting.New)
	s.err = []error{nil, nil, nil}
	s.slow = []bool{false, false, false}
	s.wg = sync.WaitGroup{}
	s.wg.Add(3)
	s.result = []int{RESULT, RESULT, RESULT}

	s.writeOperator = func(backend keyvaluestore.Backend) error {
		index := s.indexOf(backend)
		if s.mark[index] {
			s.FailNow("backend called more than one time: ", fmt.Sprint(index))
		}

		if s.slow[index] {
			<-s.continueSlowOperator
		}

		s.wg.Done()
		s.mark[index] = true

		return s.err[index]
	}

	s.readOperator = func(backend keyvaluestore.Backend) (interface{}, error) {
		index := s.indexOf(backend)
		if s.mark[index] {
			s.FailNow("backend called more than one time: ", fmt.Sprint(index))
		}

		if s.slow[index] {
			<-s.continueSlowOperator
		}

		s.wg.Done()
		s.mark[index] = true

		return s.result[index], s.err[index]
	}

	s.comparer = func(x, y interface{}) bool {
		return x.(int) == y.(int)
	}

	s.emptyRepairOperator = func(args keyvaluestore.RepairArgs) {}
}

func (s *EngineTestSuite) TearDownTest() {
	if s.engine != nil {
		s.Nil(s.engine.Close())
	}
}