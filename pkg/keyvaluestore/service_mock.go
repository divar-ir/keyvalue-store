package keyvaluestore

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type Mock_Service struct {
	mock.Mock
}

func (m *Mock_Service) Close() error {
	ret := m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Service) Set(ctx context.Context, request *SetRequest) error {
	ret := m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *SetRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Service) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	ret := m.Called(ctx, request)

	var r0 *GetResponse
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *GetRequest) *GetResponse); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *GetRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *Mock_Service) Delete(ctx context.Context, request *DeleteRequest) error {
	ret := m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *DeleteRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
