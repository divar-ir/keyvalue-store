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
	if rf, ok := ret.Get(1).(func(ctx context.Context, request *GetRequest) error); ok {
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

func (m *Mock_Service) Lock(ctx context.Context, request *LockRequest) error {
	ret := m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *LockRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Service) Unlock(ctx context.Context, request *UnlockRequest) error {
	ret := m.Called(ctx, request)

	var r0 error
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *UnlockRequest) error); ok {
		r0 = rf(ctx, request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (m *Mock_Service) Exists(ctx context.Context, request *ExistsRequest) (*ExistsResponse, error) {
	ret := m.Called(ctx, request)

	var r0 *ExistsResponse
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *ExistsRequest) *ExistsResponse); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ExistsResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(ctx context.Context, request *ExistsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *Mock_Service) GetTTL(ctx context.Context, request *GetTTLRequest) (*GetTTLResponse, error) {
	ret := m.Called(ctx, request)

	var r0 *GetTTLResponse
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *GetTTLRequest) *GetTTLResponse); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*GetTTLResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(ctx context.Context, request *GetTTLRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (m *Mock_Service) Expire(ctx context.Context, request *ExpireRequest) (*ExpireResponse, error) {
	ret := m.Called(ctx, request)

	var r0 *ExpireResponse
	if rf, ok := ret.Get(0).(func(ctx context.Context, request *ExpireRequest) *ExpireResponse); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ExpireResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(ctx context.Context, request *ExpireRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1

}
