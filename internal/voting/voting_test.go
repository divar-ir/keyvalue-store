package voting_test

import (
	"testing"

	"github.com/cafebazaar/keyvalue-store/internal/voting"
	"github.com/stretchr/testify/suite"
)

type VotingTestSuite struct {
	suite.Suite
}

func TestVotingTestSuite(t *testing.T) {
	suite.Run(t, new(VotingTestSuite))
}

func (s *VotingTestSuite) TestShouldBeInitiallyEmpty() {
	v := voting.New(s.compareInt)
	s.True(v.Empty())
}

func (s *VotingTestSuite) TestShouldNotBeEmptyAfterAddingItem() {
	v := voting.New(s.compareInt)
	v.Add(0, 0, 1)
	s.False(v.Empty())
}

func (s *VotingTestSuite) TestAddShouldReturnOneAfterFirstVote() {
	v := voting.New(s.compareInt)
	s.Equal(1, v.Add(0, 0, 1))
}

func (s *VotingTestSuite) TestAddShouldReturnTwoAfterSecondVote() {
	v := voting.New(s.compareInt)
	v.Add(0, 0, 1)
	s.Equal(2, v.Add(0, 0, 1))
}

func (s *VotingTestSuite) TestAddShouldReturnOneForNonExistingKey() {
	v := voting.New(s.compareInt)
	v.Add(0, 0, 1)
	s.Equal(1, v.Add(1, 0, 1))
}

func (s *VotingTestSuite) TestWinnersShouldReturnNilWhenEmpty() {
	v := voting.New(s.compareInt)
	s.Nil(v.Winners())
}

func (s *VotingTestSuite) TestWinnersShouldReturnDataOfMaxVote() {
	v := voting.New(s.compareInt)
	v.Add(0, "d1", 1)
	v.Add(0, "d2", 1)
	v.Add(1, "d3", 1)
	winners := v.Winners()
	s.Equal(2, len(winners))
	s.Subset(winners, []interface{}{"d1", "d2"})
}

func (s *VotingTestSuite) TestLosersShouldReturnNilWhenEmpty() {
	v := voting.New(s.compareInt)
	s.Nil(v.Losers())
}

func (s *VotingTestSuite) TestLosersShouldNotContainWinners() {
	v := voting.New(s.compareInt)
	v.Add(0, "d1", 1)
	v.Add(0, "d2", 1)
	v.Add(1, "d3", 1)
	losers := v.Losers()
	s.Equal(1, len(losers))
	s.Subset(losers, []interface{}{"d3"})
}

func (s *VotingTestSuite) TestLosersShouldAccumulateAllLosers() {
	v := voting.New(s.compareInt)
	v.Add(0, "d1", 1)
	v.Add(0, "d2", 1)
	v.Add(1, "d3", 1)
	v.Add(2, "d4", 1)
	losers := v.Losers()
	s.Equal(2, len(losers))
	s.Subset(losers, []interface{}{"d3", "d4"})
}

func (s *VotingTestSuite) TestMaxVoteShouldReturnZeroInitially() {
	value, n := voting.New(s.compareInt).MaxVote()
	s.Nil(value)
	s.Zero(n)
}

func (s *VotingTestSuite) TestMaxVoteShouldReturnWinners() {
	v := voting.New(s.compareInt)
	v.Add(0, "d1", 1)
	v.Add(0, "d2", 1)
	v.Add(1, "d3", 1)
	max, n := v.MaxVote()
	s.Equal(0, max)
	s.Equal(2, n)
}

func (s *VotingTestSuite) TestAddShouldSumWeight() {
	v := voting.New(s.compareInt)
	s.Equal(1, v.Add(0, "d1", 1))
	s.Equal(3, v.Add(0, "d2", 2))
}

func (s *VotingTestSuite) TestAddWithWeightZeroShouldAlwaysBeLoser() {
	v := voting.New(s.compareInt)
	v.Add(0, "d1", 0)
	v.Add(0, "d2", 0)
	v.Add(1, "d3", 1)
	max, n := v.MaxVote()
	s.Equal(1, max)
	s.Equal(1, n)
	winners := v.Winners()
	s.Equal(1, len(winners))
	s.Subset(winners, []interface{}{"d3"})
	losers := v.Losers()
	s.Equal(2, len(losers))
	s.Subset(losers, []interface{}{"d1", "d2"})
}

func (s *VotingTestSuite) compareInt(x, y interface{}) bool {
	return x.(int) == y.(int)
}
