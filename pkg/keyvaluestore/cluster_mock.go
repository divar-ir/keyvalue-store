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

func (m *Mock_Cluster) Read(key string, consistency ConsistencyLevel) (ReadClusterView, error) {
	ret := m.Called(key, consistency)

	var r0 ReadClusterView
	if rf, ok := ret.Get(0).(func(key string, consistency ConsistencyLevel) ReadClusterView); ok {
		r0 = rf(key, consistency)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(ReadClusterView)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(key string, consistency ConsistencyLevel) error); ok {
		r1 = rf(key, consistency)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *Mock_Cluster) Write(key string, consistency ConsistencyLevel) (WriteClusterView, error) {
	ret := m.Called(key, consistency)

	var r0 WriteClusterView
	if rf, ok := ret.Get(0).(func(key string, consistency ConsistencyLevel) WriteClusterView); ok {
		r0 = rf(key, consistency)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(WriteClusterView)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(key string, consistency ConsistencyLevel) error); ok {
		r1 = rf(key, consistency)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *Mock_Cluster) FlushDB() (WriteClusterView, error) {
	ret := m.Called()

	var r0 WriteClusterView
	if rf, ok := ret.Get(0).(func() WriteClusterView); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(WriteClusterView)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
