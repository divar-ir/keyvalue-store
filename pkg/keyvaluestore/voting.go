package keyvaluestore

type ValueComparer func(x, y interface{}) bool

type Voting interface {
	Add(value interface{}, data interface{}) int
	Empty() bool
	Losers() []interface{}
	Winners() []interface{}
	MaxVote() (interface{}, int)
}

type VotingFactory func(cmp ValueComparer) Voting
