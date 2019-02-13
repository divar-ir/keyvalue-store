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

type commandExecutionError struct {
	err error
}

func (e *commandExecutionError) Error() string {
	return e.err.Error()
}

func wrapStringAsError(msg string, args ...interface{}) error {
	return &commandExecutionError{
		err: fmt.Errorf(fmt.Sprintf(msg, args...)),
	}
}

func wrapError(err error) error {
	return &commandExecutionError{err: err}
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
			logrus.WithError(err).Error("unexpected protocol error")

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

	case "SETNX":
		err = s.handleSetNXCommand(command, writer)

	default:
		logrus.WithField("cmd", cmd).Error("command not supported")

		err = wrapStringAsError("command not supported: %v", cmd)
	}

	if err != nil {
		if execErr, ok := err.(*commandExecutionError); ok {
			err = writer.WriteError(execErr.Error())
			if err != nil {
				return err
			}
		} else {
			return err
		}
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
	nx := false

	if command.ArgCount() < 3 {
		return wrapStringAsError("expected at least 3 arguments for SET command")
	}

	for i := 3; i < command.ArgCount(); i++ {
		arg := strings.ToUpper(string(command.Get(i)))

		switch arg {
		case "EX":
			if i+1 >= command.ArgCount() {
				return wrapStringAsError("expected another arg for EX subcommand in SET")
			}
			expirationTime, err := strconv.Atoi(string(command.Get(i + 1)))
			i = i + 1
			if err != nil {
				return wrapError(err)
			}
			expiration = time.Duration(expirationTime) * time.Second

		case "PX":
			if i+1 >= command.ArgCount() {
				return wrapStringAsError("expected another arg for EX subcommand in SET")
			}
			expirationTime, err := strconv.Atoi(string(command.Get(i + 1)))
			i = i + 1
			if err != nil {
				return wrapError(err)
			}
			expiration = time.Duration(expirationTime) * time.Millisecond

		case "NX":
			nx = true

		default:
			logrus.WithField("arg", arg).Error("unsupported SET argument")

			return wrapStringAsError("unsupported SET argument: %v", arg)
		}
	}

	var err error

	if !nx {
		request := &keyvaluestore.SetRequest{
			Key:        key,
			Data:       value,
			Expiration: expiration,
			Options: keyvaluestore.WriteOptions{
				Consistency: s.writeConsistency,
			},
		}

		err = s.core.Set(context.Background(), request)
		if err != nil {
			return wrapError(err)
		}
	} else {
		request := &keyvaluestore.LockRequest{
			Key:        key,
			Expiration: expiration,
			Options: keyvaluestore.WriteOptions{
				Consistency: s.writeConsistency,
			},
		}

		err = s.core.Lock(context.Background(), request)
		if err != nil {
			return wrapError(err)
		}
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
			return wrapError(err)
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

		return wrapError(err)
	}

	if result == nil || result.Data == nil {
		return wrapStringAsError("result is nil or does not contain data: %v", result)
	}

	return writer.WriteBulk(result.Data)
}

func (s *redisServer) handlePingCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	if command.ArgCount() > 2 {
		return wrapStringAsError("expected 1-2 arguments for Ping command")
	}

	if command.ArgCount() == 1 {
		return writer.WriteSimpleString("PONG")
	}

	return writer.WriteBulk(command.Get(1))
}

func (s *redisServer) handleEchoCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	if command.ArgCount() != 2 {
		return wrapStringAsError("expected 2 arguments for Echo command")
	}

	return writer.WriteBulk(command.Get(1))
}

func (s *redisServer) handleSetNXCommand(command *redisproto.Command, writer *redisproto.Writer) error {
	if command.ArgCount() != 3 {
		return wrapStringAsError("expected 3 arguments for SetNX command")
	}

	key := string(command.Get(1))
	request := &keyvaluestore.LockRequest{
		Key: key,
		Options: keyvaluestore.WriteOptions{
			Consistency: s.writeConsistency,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := s.core.Lock(ctx, request)
	if err != nil {
		if err == context.DeadlineExceeded {
			return writer.WriteInt(0)
		}

		return wrapError(err)
	}

	return writer.WriteInt(1)
}
