package redis_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/phayes/freeport"

	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"

	"github.com/cafebazaar/keyvalue-store/internal/transport/redis"
	redisClient "github.com/go-redis/redis"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	Key        = "mykey"
	AnotherKey = "yet-another-key"
	VALUE      = "Hello, World!"
)

var (
	CONSISTENCY = keyvaluestore.ConsistencyLevel_MAJORITY
)

type RedisTransportTestSuite struct {
	suite.Suite

	port   int
	server keyvaluestore.Server
}

func TestRedisTransportTestSuite(t *testing.T) {
	suite.Run(t, new(RedisTransportTestSuite))
}

func (s *RedisTransportTestSuite) TestSetShouldProvideDataAsBinary() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.SetRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)
		s.Equal(VALUE, string(request.Data))

		return Key == request.Key && VALUE == string(request.Data)
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	s.Nil(client.Set(Key, VALUE, 0).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestSetShouldProvideConsistency() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.SetRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)
		s.Equal(CONSISTENCY, request.Options.Consistency)

		return Key == request.Key && CONSISTENCY == request.Options.Consistency
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	s.Nil(client.Set(Key, VALUE, 0).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestSetShouldProvideExpiration() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.SetRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)
		s.Equal(1*time.Minute, request.Expiration)

		return Key == request.Key && (1*time.Minute) == request.Expiration
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	s.Nil(client.Set(Key, VALUE, 1*time.Minute).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestSetShouldProvideNilExpirationIfZero() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.SetRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)
		s.Zero(request.Expiration)

		return Key == request.Key && 0 == request.Expiration
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	s.Nil(client.Set(Key, VALUE, 0).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestSetShouldReturnErrorFromEndpoint() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.SetRequest) bool {
		defer wg.Done()
		return true
	})).Return(errors.New("some error"))

	s.runServer(core)
	client := s.makeClient()
	s.NotNil(client.Set(Key, VALUE, 0).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestDeleteShouldProvideConsistency() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Delete", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.DeleteRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)
		s.Equal(CONSISTENCY, request.Options.Consistency)

		return Key == request.Key && CONSISTENCY == request.Options.Consistency
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	result, err := client.Del(Key).Result()
	s.Nil(err)
	s.Equal(1, int(result))
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestDeleteShouldSupportMultipleKeys() {
	var wg sync.WaitGroup
	wg.Add(2)

	seenKeys := map[string]bool{}

	core := &keyvaluestore.Mock_Service{}
	core.On("Delete", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.DeleteRequest) bool {
		defer wg.Done()

		seenKeys[request.Key] = true

		return true
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	result, err := client.Del(Key, AnotherKey).Result()
	s.Nil(err)
	s.Equal(2, int(result))
	wg.Wait()
	s.Equal(2, len(seenKeys))
	s.True(seenKeys[Key])
	s.True(seenKeys[AnotherKey])
}

func (s *RedisTransportTestSuite) TestDeleteShouldProcessEndpointError() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Delete", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.DeleteRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)
		return Key == request.Key
	})).Return(errors.New("some error"))

	s.runServer(core)
	client := s.makeClient()
	_, err := client.Del(Key).Result()
	s.NotNil(err)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestGetShouldBeAbleToReturnNotFoundError() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.GetRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)

		return Key == request.Key
	})).Return(nil, status.Error(codes.NotFound, ""))

	s.runServer(core)
	client := s.makeClient()
	_, err := client.Get(Key).Bytes()
	s.Equal(err, redisClient.Nil)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestGetShouldBeAbleToReturnBinaryData() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.GetRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)

		return Key == request.Key
	})).Return(&keyvaluestore.GetResponse{
		Data: []byte(VALUE),
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	response, err := client.Get(Key).Result()
	s.Nil(err)
	s.Equal(VALUE, response)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestGetShouldProvideConsistency() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.GetRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)
		s.Equal(CONSISTENCY, request.Options.Consistency)

		return Key == request.Key && CONSISTENCY == request.Options.Consistency
	})).Return(&keyvaluestore.GetResponse{
		Data: []byte(VALUE),
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	_, err := client.Get(Key).Result()
	s.Nil(err)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestGetShouldReturnEndpointError() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.GetRequest) bool {
		defer wg.Done()

		s.Equal(Key, request.Key)
		return Key == request.Key
	})).Return(nil, errors.New("some error"))

	s.runServer(core)
	client := s.makeClient()
	_, err := client.Get(Key).Result()
	s.NotNil(err)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestShouldSupportPingCommnand() {
	core := &keyvaluestore.Mock_Service{}

	s.runServer(core)
	client := s.makeClient()
	value, err := client.Ping().Result()
	s.Nil(err)
	s.Equal("PONG", value)
}

func (s *RedisTransportTestSuite) TestShouldSupportEchoCommand() {
	core := &keyvaluestore.Mock_Service{}

	s.runServer(core)
	client := s.makeClient()
	value, err := client.Echo("hello").Result()
	s.Nil(err)
	s.Equal("hello", value)
}

func (s *RedisTransportTestSuite) TestShouldSupportSetIfNotSetViaLocking() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Lock", mock.Anything, mock.MatchedBy(func(lockRequest *keyvaluestore.LockRequest) bool {
		defer wg.Done()

		s.Equal(VALUE, string(lockRequest.Data))
		return VALUE == string(lockRequest.Data)
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	ok, err := client.SetNX(Key, VALUE, 0).Result()
	s.Nil(err)
	s.True(ok)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestShouldConsiderUnavailableAsSetNXZeroResult() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Lock", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.LockRequest) bool {
		defer wg.Done()

		return true
	})).Once().Return(status.Error(codes.Unavailable, "consistency error"))

	s.runServer(core)
	client := s.makeClient()
	ok, err := client.SetNX(Key, VALUE, 0).Result()
	s.Nil(err)
	s.False(ok)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestExpireShouldCountExistingKeyAsIntergerOne() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Expire", mock.Anything, mock.MatchedBy(func(expireRequest *keyvaluestore.ExpireRequest) bool {
		defer wg.Done()
		return true
	})).Once().Return(&keyvaluestore.ExpireResponse{
		Exists: true,
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	ok, err := client.Expire(Key, 1*time.Minute).Result()
	s.Nil(err)
	s.Equal(true, ok)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestExpireShouldCountNonExistingKeyAsIntergerZero() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Expire", mock.Anything, mock.MatchedBy(func(existsRequest *keyvaluestore.ExpireRequest) bool {
		defer wg.Done()
		return true
	})).Once().Return(&keyvaluestore.ExpireResponse{
		Exists: false,
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	ok, err := client.Expire(Key, 1*time.Minute).Result()
	s.Nil(err)
	s.Equal(false, ok)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestExistsShouldCountExistingKeyAsIntergerOne() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Exists", mock.Anything, mock.MatchedBy(func(existsRequest *keyvaluestore.ExistsRequest) bool {
		defer wg.Done()
		return true
	})).Once().Return(&keyvaluestore.ExistsResponse{
		Exists: true,
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	count, err := client.Exists(Key).Result()
	s.Nil(err)
	s.Equal(int64(1), count)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestExistsShouldCountNonExistingKeyAsIntergerZero() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Exists", mock.Anything, mock.MatchedBy(func(existsRequest *keyvaluestore.ExistsRequest) bool {
		defer wg.Done()
		return true
	})).Once().Return(&keyvaluestore.ExistsResponse{
		Exists: false,
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	count, err := client.Exists(Key).Result()
	s.Nil(err)
	s.Equal(int64(0), count)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestTTLShouldConsiderNonExistingKeyAsIntegerMinusTwo() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("GetTTL", mock.Anything, mock.MatchedBy(func(getTTLRequest *keyvaluestore.GetTTLRequest) bool {
		defer wg.Done()
		return true
	})).Once().Return(nil, status.Error(codes.NotFound, "not found"))

	s.runServer(core)
	client := s.makeClient()
	duration, err := client.TTL(Key).Result()
	s.Nil(err)
	s.Equal(-2*time.Second, duration)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestTTLShouldConsiderNonExipyKeyAsIntegerMinusOne() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("GetTTL", mock.Anything, mock.MatchedBy(func(getTTLRequest *keyvaluestore.GetTTLRequest) bool {
		defer wg.Done()
		return true
	})).Once().Return(&keyvaluestore.GetTTLResponse{}, nil)

	s.runServer(core)
	client := s.makeClient()
	duration, err := client.TTL(Key).Result()
	s.Nil(err)
	s.Equal(-1*time.Second, duration)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestTTLShouldReturnNumberOfSeconds() {
	var wg sync.WaitGroup
	wg.Add(1)

	oneMinute := 1 * time.Minute

	core := &keyvaluestore.Mock_Service{}
	core.On("GetTTL", mock.Anything, mock.MatchedBy(func(getTTLRequest *keyvaluestore.GetTTLRequest) bool {
		defer wg.Done()
		return true
	})).Once().Return(&keyvaluestore.GetTTLResponse{
		TTL: &oneMinute,
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	duration, err := client.TTL(Key).Result()
	s.Nil(err)
	s.Equal(1*time.Minute, duration)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestMSetShouldSetMultipleKeys() {
	var wg sync.WaitGroup
	wg.Add(3)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(setRequest *keyvaluestore.SetRequest) bool {
		return setRequest.Key == "A"
	})).Times(1).Run(func(args mock.Arguments) {
		wg.Done()
	}).Return(nil)

	core.On("Set", mock.Anything, mock.MatchedBy(func(setRequest *keyvaluestore.SetRequest) bool {
		return setRequest.Key == "B"
	})).Times(1).Run(func(args mock.Arguments) {
		wg.Done()
	}).Return(nil)

	core.On("Set", mock.Anything, mock.MatchedBy(func(setRequest *keyvaluestore.SetRequest) bool {
		return setRequest.Key == "C"
	})).Times(1).Run(func(args mock.Arguments) {
		wg.Done()
	}).Return(nil)

	s.runServer(core)
	client := s.makeClient()

	err := client.MSet("A", 1, "B", 2, "C", 3).Err()
	s.Nil(err)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestMGetShouldReturnNilInMiddleOfKeys() {
	var wg sync.WaitGroup
	wg.Add(3)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(getRequest *keyvaluestore.GetRequest) bool {
		return getRequest.Key == "A" || getRequest.Key == "C"
	})).Times(2).Return(&keyvaluestore.GetResponse{
		Data: []byte(VALUE),
	}, nil).Run(func(args mock.Arguments) {
		wg.Done()
	})
	core.On("Get", mock.Anything, mock.MatchedBy(func(getRequest *keyvaluestore.GetRequest) bool {
		return getRequest.Key == "B"
	})).Times(1).Run(func(args mock.Arguments) {
		wg.Done()
	}).Return(nil, status.Error(codes.NotFound, "key not found"))

	s.runServer(core)
	client := s.makeClient()

	result, err := client.MGet("A", "B", "C").Result()
	s.Nil(err)

	s.Equal(3, len(result))
	s.Equal(VALUE, result[0])
	s.Nil(result[1])
	s.Equal(VALUE, result[2])
	wg.Wait()
}

func (s *RedisTransportTestSuite) runServer(core keyvaluestore.Service) {
	s.server = redis.New(core, s.port, CONSISTENCY, CONSISTENCY)
	s.Nil(s.server.Start())
}

func (s *RedisTransportTestSuite) makeClient() *redisClient.Client {
	return redisClient.NewClient(&redisClient.Options{Addr: fmt.Sprintf("127.0.0.1:%d", s.port)})
}

func (s *RedisTransportTestSuite) SetupTest() {
	var err error

	s.port, err = freeport.GetFreePort()
	s.Nil(err)
	s.server = nil
}

func (s *RedisTransportTestSuite) TearDownTest() {
	if s.server != nil {
		s.Nil(s.server.Close())
	}
}
