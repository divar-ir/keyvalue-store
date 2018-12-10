package keyvaluestore

import "github.com/stretchr/testify/mock"

type Mock_Cluster struct {
	mock.Mock
}

func (m *Mock_Cluster) Close() error {
	ret := m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Cluster) ReadBackends(key string, consistency ConsistencyLevel) []Backend {
	ret := m.Called(key, consistency)

	var r0 []Backend
	if rf, ok := ret.Get(0).(func(key string, consistency ConsistencyLevel) []Backend); ok {
		r0 = rf(key, consistency)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]Backend)
		}
	}

	return r0
}

func (m *Mock_Cluster) ReadVoteRequired(key string, consistency ConsistencyLevel) int {
	ret := m.Called(key, consistency)

	var r0 int
	if rf, ok := ret.Get(0).(func(key string, consistency ConsistencyLevel) int); ok {
		r0 = rf(key, consistency)
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

func (m *Mock_Cluster) WriteBackends(key string, consistency ConsistencyLevel) []Backend {
	ret := m.Called(key, consistency)

	var r0 []Backend
	if rf, ok := ret.Get(0).(func(key string, consistency ConsistencyLevel) []Backend); ok {
		r0 = rf(key, consistency)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]Backend)
		}
	}

	return r0
}

func (m *Mock_Cluster) WriteAcknowledgeRequired(key string, consistency ConsistencyLevel) int {
	ret := m.Called(key, consistency)

	var r0 int
	if rf, ok := ret.Get(0).(func(key string, consistency ConsistencyLevel) int); ok {
		r0 = rf(key, consistency)
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}
