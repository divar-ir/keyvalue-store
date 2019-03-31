package voting

import (
	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"
)

type voting struct {
	items    []*voteItem
	comparer keyvaluestore.ValueComparer
}

type voteItem struct {
	value interface{}
	vote  int
	data  []interface{}
}

func New(comparer keyvaluestore.ValueComparer) keyvaluestore.Voting {
	return &voting{comparer: comparer}
}

func (v *voting) Add(value interface{}, data interface{}, weight int) int {
	for _, item := range v.items {
		if v.comparer(value, item.value) {
			item.vote = item.vote + weight
			item.data = append(item.data, data)
			return item.vote
		}
	}

	v.items = append(v.items, &voteItem{
		value: value,
		vote:  weight,
		data:  []interface{}{data},
	})

	return weight
}

func (v *voting) Empty() bool {
	return len(v.items) == 0
}

func (v *voting) Losers() []interface{} {
	maxVoteItemItem := v.maxVoteItem()
	if maxVoteItemItem == nil {
		return nil
	}

	var result []interface{}

	for _, item := range v.items {
		if item.vote != maxVoteItemItem.vote {
			result = append(result, item.data...)
		}
	}

	return result
}

func (v *voting) Winners() []interface{} {
	maxVoteItemItem := v.maxVoteItem()
	if maxVoteItemItem == nil {
		return nil
	}

	return maxVoteItemItem.data
}

func (v *voting) MaxVote() (interface{}, int) {
	result := v.maxVoteItem()
	if result == nil {
		return nil, 0
	}

	return result.value, result.vote
}

func (v *voting) maxVoteItem() *voteItem {
	var result *voteItem
	vote := 0

	for _, item := range v.items {
		if item.vote > vote {
			vote = item.vote
			result = item
		}
	}

	return result
}
