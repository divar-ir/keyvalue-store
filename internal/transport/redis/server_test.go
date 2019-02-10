package redis_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/phayes/freeport"

	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"

	"github.com/cafebazaar/keyvalue-store/internal/transport/redis"
	redisClient "github.com/go-redis/redis"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	KEY         = "mykey"
	ANOTHER_KEY = "yet-another-key"
	VALUE       = "Hello, World!"
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

		s.Equal(KEY, request.Key)
		s.Equal(VALUE, string(request.Data))

		return KEY == request.Key && VALUE == string(request.Data)
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	s.Nil(client.Set(KEY, VALUE, 0).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestSetShouldProvideConsistency() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.SetRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)
		s.Equal(CONSISTENCY, request.Options.Consistency)

		return KEY == request.Key && CONSISTENCY == request.Options.Consistency
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	s.Nil(client.Set(KEY, VALUE, 0).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestSetShouldProvideExpiration() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.SetRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)
		s.Equal(1*time.Minute, request.Expiration)

		return KEY == request.Key && (1*time.Minute) == request.Expiration
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	s.Nil(client.Set(KEY, VALUE, 1*time.Minute).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestSetShouldProvideNilExpirationIfZero() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Set", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.SetRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)
		s.Zero(request.Expiration)

		return KEY == request.Key && 0 == request.Expiration
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	s.Nil(client.Set(KEY, VALUE, 0).Err())
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
	s.NotNil(client.Set(KEY, VALUE, 0).Err())
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestDeleteShouldProvideConsistency() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Delete", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.DeleteRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)
		s.Equal(CONSISTENCY, request.Options.Consistency)

		return KEY == request.Key && CONSISTENCY == request.Options.Consistency
	})).Return(nil)

	s.runServer(core)
	client := s.makeClient()
	result, err := client.Del(KEY).Result()
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
	result, err := client.Del(KEY, ANOTHER_KEY).Result()
	s.Nil(err)
	s.Equal(2, int(result))
	wg.Wait()
	s.Equal(2, len(seenKeys))
	s.True(seenKeys[KEY])
	s.True(seenKeys[ANOTHER_KEY])
}

func (s *RedisTransportTestSuite) TestDeleteShouldProcessEndpointError() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Delete", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.DeleteRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)
		return KEY == request.Key
	})).Return(errors.New("some error"))

	s.runServer(core)
	client := s.makeClient()
	_, err := client.Del(KEY).Result()
	s.NotNil(err)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestGetShouldBeAbleToReturnNotFoundError() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.GetRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)

		return KEY == request.Key
	})).Return(nil, keyvaluestore.ErrNotFound)

	s.runServer(core)
	client := s.makeClient()
	_, err := client.Get(KEY).Bytes()
	s.Equal(err, redisClient.Nil)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestGetShouldBeAbleToReturnBinaryData() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.GetRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)

		return KEY == request.Key
	})).Return(&keyvaluestore.GetResponse{
		Data: []byte(VALUE),
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	response, err := client.Get(KEY).Result()
	s.Nil(err)
	s.Equal(VALUE, string(response))
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestGetShouldProvideConsistency() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.GetRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)
		s.Equal(CONSISTENCY, request.Options.Consistency)

		return KEY == request.Key && CONSISTENCY == request.Options.Consistency
	})).Return(&keyvaluestore.GetResponse{
		Data: []byte(VALUE),
	}, nil)

	s.runServer(core)
	client := s.makeClient()
	_, err := client.Get(KEY).Result()
	s.Nil(err)
	wg.Wait()
}

func (s *RedisTransportTestSuite) TestGetShouldReturnEndpointError() {
	var wg sync.WaitGroup
	wg.Add(1)

	core := &keyvaluestore.Mock_Service{}
	core.On("Get", mock.Anything, mock.MatchedBy(func(request *keyvaluestore.GetRequest) bool {
		defer wg.Done()

		s.Equal(KEY, request.Key)
		return KEY == request.Key
	})).Return(nil, errors.New("some error"))

	s.runServer(core)
	client := s.makeClient()
	_, err := client.Get(KEY).Result()
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
