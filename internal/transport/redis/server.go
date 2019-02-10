package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cafebazaar/keyvalue-store/pkg/keyvaluestore"

	redisproto "github.com/secmask/go-redisproto"
	"github.com/sirupsen/logrus"
)

type redisServer struct {
	listenPort       int
	core             keyvaluestore.Service
	readConsistency  keyvaluestore.ConsistencyLevel
	writeConsistency keyvaluestore.ConsistencyLevel
	wg               sync.WaitGroup
	listener         net.Listener
}

func New(core keyvaluestore.Service, listenPort int,
	readConsistency keyvaluestore.ConsistencyLevel,
	writeConsistency keyvaluestore.ConsistencyLevel) keyvaluestore.Server {

	return &redisServer{
		core:             core,
		listenPort:       listenPort,
		readConsistency:  readConsistency,
		writeConsistency: writeConsistency,
	}
}

func (s *redisServer) Start() error {
	var err error

	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.listenPort))
	if err != nil {
		return err
	}

	started := make(chan struct{})
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		close(started)

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				return
			}

			go s.handleConnection(conn)
		}
	}()
	<-started

	return nil
}

func (s *redisServer) Close() error {
	err := s.listener.Close()
	s.wg.Wait()
	return err
}

func (s *redisServer) handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			logrus.WithError(err).Info("unexpected error while closing connection")
		}
	}()

	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(bufio.NewWriter(conn))

	for {
		if err := s.connectionLoop(parser, writer); err != nil {
			logrus.WithError(err).Info("unexpected error while handling connection")
			return
		}
	}
}

func (s *redisServer) connectionLoop(parser *redisproto.Parser, writer *redisproto.Writer) error {
	command, err := parser.ReadCommand()
	if err != nil {
		_, ok := err.(*redisproto.ProtocolError)
		if ok {
			return writer.WriteError(err.Error())
		}

		return keyvaluestore.ErrClosed
	}

	return s.dispatchCommand(command, writer)
}

func (s *redisServer) dispatchCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	cmd := strings.ToUpper(string(command.Get(0)))
	var err error

	switch cmd {
	case "SET":
		err = s.handleSetCommand(command, writer)

	case "DEL":
		err = s.handleDeleteCommand(command, writer)

	case "GET":
		err = s.handleGetCommand(command, writer)

	case "PING":
		err = s.handlePingCommand(command, writer)

	case "ECHO":
		err = s.handleEchoCommand(command, writer)

	default:
		err = writer.WriteError(fmt.Sprintf("command not supported: %v", cmd))
	}

	if err != nil {
		return err
	}

	if command.IsLast() {
		return writer.Flush()
	}

	return nil
}

func (s *redisServer) handleSetCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	key := string(command.Get(1))
	value := command.Get(2)
	var expiration time.Duration

	if command.ArgCount() < 3 {
		return writer.WriteError("expected at least 3 arguments for SET command")
	}

	if command.ArgCount() > 3 {
		expirationMode := strings.ToUpper(string(command.Get(3)))
		expirationTime, err := strconv.Atoi(string(command.Get(4)))
		if err != nil {
			return writer.WriteError(err.Error())
		}

		switch expirationMode {
		case "EX":
			expiration = time.Duration(expirationTime) * time.Second

		case "PX":
			expiration = time.Duration(expirationTime) * time.Millisecond

		default:
			return writer.WriteError(fmt.Sprintf("unsupported expiration mode: %v", expirationMode))
		}
	}

	request := &keyvaluestore.SetRequest{
		Key:        key,
		Data:       value,
		Expiration: expiration,
		Options: keyvaluestore.WriteOptions{
			Consistency: s.writeConsistency,
		},
	}

	err := s.core.Set(context.Background(), request)
	if err != nil {
		return writer.WriteError(err.Error())
	}

	return writer.WriteBulkString("OK")
}

func (s *redisServer) handleDeleteCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	for i := 1; i < command.ArgCount(); i++ {
		key := string(command.Get(i))

		request := &keyvaluestore.DeleteRequest{
			Key: key,
			Options: keyvaluestore.WriteOptions{
				Consistency: s.writeConsistency,
			},
		}

		err := s.core.Delete(context.Background(), request)
		if err != nil {
			return writer.WriteError(err.Error())
		}
	}

	return writer.WriteInt(int64(command.ArgCount() - 1))
}

func (s *redisServer) handleGetCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	key := string(command.Get(1))

	request := &keyvaluestore.GetRequest{
		Key: key,
		Options: keyvaluestore.ReadOptions{
			Consistency: s.readConsistency,
		},
	}

	result, err := s.core.Get(context.Background(), request)
	if err != nil {
		if err == keyvaluestore.ErrNotFound {
			return writer.WriteBulk(nil)
		}

		return writer.WriteError(err.Error())
	}

	if result == nil || result.Data == nil {
		return writer.WriteError(fmt.Sprintf("result is nil or does not contain data: %v", result))
	}

	return writer.WriteBulk(result.Data)
}

func (s *redisServer) handlePingCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	if command.ArgCount() > 2 {
		return writer.WriteError("expected 1-2 arguments for Ping command")
	}

	if command.ArgCount() == 1 {
		return writer.WriteSimpleString("PONG")
	}

	return writer.WriteBulk(command.Get(1))
}

func (s *redisServer) handleEchoCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	if command.ArgCount() != 2 {
		return writer.WriteError("expected 2 arguments for Echo command")
	}

	return writer.WriteBulk(command.Get(1))
}
