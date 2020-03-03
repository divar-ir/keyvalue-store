package keyvaluestore

import (
	"time"

	"github.com/stretchr/testify/mock"
)

type Mock_Backend struct {
	mock.Mock
}

func (m *Mock_Backend) Close() error {
	ret := m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Backend) Set(key string, value []byte, expiration time.Duration) error {
	ret := m.Called(key, value, expiration)

	var r0 error
	if rf, ok := ret.Get(0).(func(key string, value []byte, expiration time.Duration) error); ok {
		r0 = rf(key, value, expiration)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Backend) TTL(key string) (*time.Duration, error) {
	ret := m.Called(key)

	var r0 *time.Duration
	if rf, ok := ret.Get(0).(func(key string) *time.Duration); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*time.Duration)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(key string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *Mock_Backend) Get(key string) ([]byte, error) {
	ret := m.Called(key)

	var r0 []byte
	if rf, ok := ret.Get(0).(func(key string) []byte); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(key string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *Mock_Backend) Delete(key string) error {
	ret := m.Called(key)

	var r0 error
	if rf, ok := ret.Get(0).(func(key string) error); ok {
		r0 = rf(key)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Backend) Address() string {
	ret := m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(string)
		}
	}

	return r0
}

func (m *Mock_Backend) Lock(key string, value []byte, expiration time.Duration) error {
	ret := m.Called(key, value, expiration)

	var r0 error
	if rf, ok := ret.Get(0).(func(key string, value []byte, expiration time.Duration) error); ok {
		r0 = rf(key, value, expiration)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Backend) Unlock(key string) error {
	ret := m.Called(key)

	var r0 error
	if rf, ok := ret.Get(0).(func(key string) error); ok {
		r0 = rf(key)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Backend) Exists(key string) (bool, error) {
	ret := m.Called(key)

	var r0 bool
	if rf, ok := ret.Get(0).(func(key string) bool); ok {
		r0 = rf(key)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(key string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *Mock_Backend) Expire(key string, expiration time.Duration) error {
	ret := m.Called(key, expiration)

	var r0 error
	if rf, ok := ret.Get(0).(func(key string, expiration time.Duration) error); ok {
		r0 = rf(key, expiration)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Backend) FlushDB() error {
	ret := m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
