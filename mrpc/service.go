package mrpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"

	"errors"

	"github.com/tychoish/birch/mrpc/mongowire"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
	"github.com/tychoish/grip/recovery"
)

type HandlerFunc func(context.Context, io.Writer, mongowire.Message)

type Service interface {
	Address() string
	RegisterOperation(scope *mongowire.OpScope, h HandlerFunc) error
	Run(context.Context) error
}

type basicService struct {
	addr     string
	registry *OperationRegistry
}

// NewService starts a generic wire protocol service listening on the given host
// and port.
func NewBasicService(host string, port int) Service {
	return &basicService{
		addr:     fmt.Sprintf("%s:%d", host, port),
		registry: &OperationRegistry{ops: make(map[mongowire.OpScope]HandlerFunc)},
	}
}

func (s *basicService) Address() string { return s.addr }

func (s *basicService) RegisterOperation(scope *mongowire.OpScope, h HandlerFunc) error {
	return (s.registry.Add(*scope, h))
}

func (s *basicService) Run(ctx context.Context) error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("problem listening on %s: %w", s.addr, err)
	}
	defer func() { _ = l.Close() }()

	for {
		if ctx.Err() != nil {
			return nil
		}

		conn, err := l.Accept()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		} else if err != nil {
			grip.Warning(message.WrapError(err, "problem accepting connection"))
			continue
		}

		go s.dispatchRequest(ctx, conn)
	}
}

func (s *basicService) dispatchRequest(ctx context.Context, conn net.Conn) {
	defer func() {
		err := recovery.HandlePanicWithError(recover(), nil, "connection handling")
		grip.Error(message.WrapError(err, "error during request handling"))
		if err := conn.Close(); err != nil {
			grip.Error(message.WrapErrorf(err, "error closing connection from %s", conn.RemoteAddr()))
			return
		}
	}()

	if c, ok := conn.(*tls.Conn); ok {
		// we do this here so that we can get the SNI server name
		if err := c.Handshake(); err != nil {
			grip.Warning(message.WrapError(err, "error doing tls handshake"))
			return
		}
	}

	for {
		m, err := mongowire.ReadMessage(ctx, conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Connection was likely closed
				return
			}
			if ctx.Err() != nil {
				return
			}
			grip.Error(message.WrapError(err, "problem reading message"))
			return
		}

		scope := m.Scope()

		handler, ok := s.registry.Get(scope)
		if !ok {
			grip.Warningf("undefined command scope: %+v", scope)
			return
		}

		go handler(ctx, conn, m)
	}
}
