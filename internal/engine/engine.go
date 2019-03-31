package engine

import (
	"sync"

	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"
	"github.com/sirupsen/logrus"
)

type keyValueEngine struct {
	votingFactory            keyvaluestore.VotingFactory
	operating                sync.WaitGroup
	closed                   chan struct{}
	mutex                    sync.Mutex
	wg                       sync.WaitGroup
	ignoreWriteResultChannel chan asyncWriteResult
}

func New(votingFactory keyvaluestore.VotingFactory) keyvaluestore.Engine {

	result := &keyValueEngine{
		votingFactory:            votingFactory,
		ignoreWriteResultChannel: make(chan asyncWriteResult, 1024),
		closed:                   make(chan struct{}),
	}

	started := make(chan struct{})
	result.wg.Add(1)
	go result.beginLogDelivery(started)
	<-started

	return result
}

type asyncReadResult struct {
	value interface{}
	err   error
	node  keyvaluestore.Backend
}

type asyncWriteResult struct {
	err  error
	node keyvaluestore.Backend
}

type voteItem struct {
	notFound bool
	value    interface{}
}

func (e *keyValueEngine) Read(nodes []keyvaluestore.Backend,
	votesRequired int,
	operator keyvaluestore.ReadOperator,
	repair keyvaluestore.RepairOperator,
	cmp keyvaluestore.ValueComparer,
	mode keyvaluestore.VotingMode) (interface{}, error) {

	e.operating.Add(1)
	defer e.operating.Done()

	if e.isClosed() {
		return nil, keyvaluestore.ErrClosed
	}

	var wg sync.WaitGroup
	resultChannel := make(chan asyncReadResult, len(nodes))

	e.startReadOperatorOnMultipleNodes(nodes, operator, &wg, resultChannel)
	voteChannel := e.startReadVote(&wg, resultChannel, cmp, votesRequired, repair, mode)

	vote := <-voteChannel
	return vote.value, vote.err
}

func (e *keyValueEngine) Write(nodes []keyvaluestore.Backend,
	acknowledgeRequired int,
	operator keyvaluestore.WriteOperator,
	rollback keyvaluestore.RollbackOperator,
	mode keyvaluestore.OperationMode) error {

	var wg sync.WaitGroup
	resultChannel := make(chan asyncWriteResult, len(nodes))

	e.startWriteOperatorOnMultipleNodes(nodes, operator, &wg, resultChannel, mode)
	completedChannel := e.startWaitingForWriteCompletion(&wg, resultChannel, rollback, acknowledgeRequired)

	result := <-completedChannel
	return result.err
}

func (e *keyValueEngine) startWaitingForWriteCompletion(wg *sync.WaitGroup,
	resultChannel chan asyncWriteResult,
	rollback keyvaluestore.RollbackOperator,
	requiredNodes int) chan asyncWriteResult {

	ch := make(chan asyncWriteResult, 1)
	e.operating.Add(1)
	go e.waitForWriteCompletion(wg, resultChannel, requiredNodes, rollback, ch)
	return ch
}

func (e *keyValueEngine) startReadVote(wg *sync.WaitGroup,
	resultChannel chan asyncReadResult,
	comparer keyvaluestore.ValueComparer,
	requiredVotes int,
	repair keyvaluestore.RepairOperator,
	mode keyvaluestore.VotingMode) chan asyncReadResult {

	ch := make(chan asyncReadResult, 1)
	e.operating.Add(1)
	go e.waitForReadVote(wg, resultChannel, comparer, requiredVotes, repair, mode, ch)

	return ch
}

func (e *keyValueEngine) waitForWriteCompletion(wg *sync.WaitGroup,
	resultChannel chan asyncWriteResult,
	requiredNodes int,
	rollback keyvaluestore.RollbackOperator,
	finalResultChannel chan asyncWriteResult) {

	defer e.operating.Done()

	done := e.beginWaitGroupMonitor(wg)
	completed := 0
	var lastErr error
	var completedNodes []keyvaluestore.Backend

	if requiredNodes == 0 {
		finalResultChannel <- asyncWriteResult{err: nil}
		close(finalResultChannel)
		finalResultChannel = nil
	}

	for {
		select {
		case <-done:
			close(resultChannel)
			done = nil

		case result, ok := <-resultChannel:
			if !ok {
				if finalResultChannel != nil {
					if completed < requiredNodes || lastErr == nil {
						finalResultChannel <- asyncWriteResult{err: keyvaluestore.ErrConsistency}
					} else {
						finalResultChannel <- asyncWriteResult{err: lastErr}
					}

					close(finalResultChannel)

					if rollback != nil {
						rollback(keyvaluestore.RollbackArgs{
							Nodes: completedNodes,
						})
					}
				}

				return

			} else if result.err == nil {
				completed = completed + 1
				completedNodes = append(completedNodes, result.node)
				if completed >= requiredNodes && finalResultChannel != nil {
					finalResultChannel <- asyncWriteResult{err: nil}
					close(finalResultChannel)
					finalResultChannel = nil
				}
			} else {
				if lastErr != nil {
					e.logError(lastErr)
				}

				lastErr = result.err
			}
		}
	}
}

func (e *keyValueEngine) waitForReadVote(wg *sync.WaitGroup,
	everyNodeResultChannel chan asyncReadResult,
	cmp keyvaluestore.ValueComparer,
	requiredVotes int,
	repair keyvaluestore.RepairOperator,
	mode keyvaluestore.VotingMode,
	finalResultChannel chan asyncReadResult) {

	defer e.operating.Done()

	var lastErr error
	done := e.beginWaitGroupMonitor(wg)
	votes := e.votingFactory(e.makeVoteComparer(cmp))

	for {
		select {
		case <-done:
			close(everyNodeResultChannel)
			done = nil

		case result, ok := <-everyNodeResultChannel:
			if !ok {
				if finalResultChannel == nil {
					losers := votes.Losers()

					if len(losers) > 0 && repair != nil {
						maxVoteValue, _ := votes.MaxVote()
						maxVoteItem := maxVoteValue.(voteItem)

						winners := votes.Winners()

						var args keyvaluestore.RepairArgs
						if maxVoteItem.notFound {
							args.Err = keyvaluestore.ErrNotFound
						} else {
							args.Value = maxVoteItem.value
						}

						for _, winner := range winners {
							args.Winners = append(args.Winners, winner.(keyvaluestore.Backend))
						}

						for _, loser := range losers {
							args.Losers = append(args.Losers, loser.(keyvaluestore.Backend))
						}

						repair(args)
					}
				} else if !votes.Empty() || lastErr == nil {
					finalResultChannel <- asyncReadResult{err: keyvaluestore.ErrConsistency}
					close(finalResultChannel)
				} else {
					finalResultChannel <- asyncReadResult{err: lastErr}
					close(finalResultChannel)
				}

				return
			} else if result.err != nil {
				if result.err == keyvaluestore.ErrNotFound {
					var weight int

					switch mode {
					case keyvaluestore.VotingModeSkipVoteOnNotFound:
						weight = 0
					default:
						weight = 1
					}

					if votes.Add(voteItem{notFound: true}, result.node, weight) >= requiredVotes && finalResultChannel != nil {
						finalResultChannel <- asyncReadResult{err: keyvaluestore.ErrNotFound}
						close(finalResultChannel)
						finalResultChannel = nil
					}
				} else {
					if lastErr != nil {
						e.logError(lastErr)
					}

					lastErr = result.err
				}
			} else {
				if votes.Add(voteItem{value: result.value}, result.node, 1) >= requiredVotes && finalResultChannel != nil {
					finalResultChannel <- asyncReadResult{value: result.value}
					close(finalResultChannel)
					finalResultChannel = nil
				}
			}
		}
	}
}

func (e *keyValueEngine) startWriteOperatorOnMultipleNodes(nodes []keyvaluestore.Backend,
	operator keyvaluestore.WriteOperator,
	wg *sync.WaitGroup,
	resultChannel chan asyncWriteResult,
	mode keyvaluestore.OperationMode) {

	switch mode {
	case keyvaluestore.OperationModeConcurrent:
		for _, node := range nodes {
			e.performAdd(wg, 1)
			e.operating.Add(1)
			go e.performWriteOperatorOnSingleNode(node, operator, wg, resultChannel)
		}

	case keyvaluestore.OperationModeSequential:
		e.operating.Add(len(nodes))
		e.performAdd(wg, len(nodes))

		go func() {
			for _, node := range nodes {
				e.performWriteOperatorOnSingleNode(node, operator, wg, resultChannel)
			}
		}()
	}
}

func (e *keyValueEngine) startReadOperatorOnMultipleNodes(nodes []keyvaluestore.Backend,
	operator keyvaluestore.ReadOperator,
	wg *sync.WaitGroup,
	resultChannel chan asyncReadResult) {

	for _, node := range nodes {
		e.performAdd(wg, 1)
		e.operating.Add(1)
		go e.performReadOperatorOnSingleNode(node, operator, wg, resultChannel)
	}
}

func (e *keyValueEngine) performWriteOperatorOnSingleNode(node keyvaluestore.Backend,
	operator keyvaluestore.WriteOperator,
	wg *sync.WaitGroup,
	resultChannel chan asyncWriteResult) {

	defer e.makeDone(wg)
	defer e.operating.Done()

	err := operator(node)
	resultChannel <- asyncWriteResult{
		err:  err,
		node: node,
	}
}

func (e *keyValueEngine) performReadOperatorOnSingleNode(node keyvaluestore.Backend,
	operator keyvaluestore.ReadOperator,
	wg *sync.WaitGroup,
	resultChannel chan asyncReadResult) {

	defer e.makeDone(wg)
	defer e.operating.Done()

	value, err := operator(node)
	resultChannel <- asyncReadResult{
		value: value,
		err:   err,
		node:  node,
	}
}

func (e *keyValueEngine) Close() error {
	if closed := e.setClosed(); !closed {
		return nil
	}

	e.operating.Wait()
	e.wg.Wait()

	return nil
}

func (e *keyValueEngine) beginLogDelivery(started chan struct{}) {
	defer e.wg.Done()

	close(started)

	for {
		select {
		case <-e.closed:
			return

		case result := <-e.ignoreWriteResultChannel:
			if result.err != nil {
				e.logError(result.err)
			}
		}
	}
}

func (e *keyValueEngine) setClosed() bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.isClosedWithoutLock() {
		return false
	}

	close(e.closed)
	return true
}

func (e *keyValueEngine) isClosed() bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return e.isClosedWithoutLock()
}

func (e *keyValueEngine) isClosedWithoutLock() bool {
	select {
	case <-e.closed:
		return true

	default:
		return false
	}
}

func (e *keyValueEngine) beginWaitGroupMonitor(wg *sync.WaitGroup) chan struct{} {
	result := make(chan struct{})
	e.operating.Add(1)
	go func() {
		defer e.operating.Done()

		wg.Wait()
		close(result)
	}()

	return result
}

func (e *keyValueEngine) logError(err error) {
	logrus.WithError(err).Error("redis error")
}

func (e *keyValueEngine) performAdd(wg *sync.WaitGroup, delta int) {
	if wg != nil {
		wg.Add(delta)
	}
}

func (e *keyValueEngine) makeDone(wg *sync.WaitGroup) {
	if wg != nil {
		wg.Done()
	}
}

func (e *keyValueEngine) makeVoteComparer(cmp keyvaluestore.ValueComparer) keyvaluestore.ValueComparer {
	return func(x, y interface{}) bool {
		left := x.(voteItem)
		right := y.(voteItem)

		if left.notFound != right.notFound {
			return false
		}

		if left.notFound {
			return true
		}

		return cmp(left.value, right.value)
	}
}
