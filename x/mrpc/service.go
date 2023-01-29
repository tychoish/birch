package mrpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"

	"errors"

	"github.com/tychoish/birch/x/mrpc/mongowire"
)

type HandlerFunc func(context.Context, io.Writer, mongowire.Message)

type Service interface {
	Address() string
	RegisterOperation(scope *mongowire.OpScope, h HandlerFunc) error
	Run(context.Context) error
	RegisterErrorHandler(func(error))
}

type basicService struct {
	addr          string
	registry      *OperationRegistry
	errorHandlers []func(error)
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
func (s *basicService) RegisterErrorHandler(fn func(error)) {
	s.errorHandlers = append(s.errorHandlers, fn)
}

func (s *basicService) handleError(err error) {
	if err == nil {
		return
	}
	for _, fn := range s.errorHandlers {
		fn(err)
	}
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
			s.handleError(fmt.Errorf("accepting connection: %w", err))
			continue
		}

		go s.dispatchRequest(ctx, conn)
	}
}

func (s *basicService) dispatchRequest(ctx context.Context, conn net.Conn) {
	defer func() {
		if p := recover(); p != nil {
			s.handleError(fmt.Errorf("panic responding to request: %v", p))
		}

		if err := conn.Close(); err != nil {
			s.handleError(fmt.Errorf("error closing connection from %q: %w", conn.RemoteAddr(), err))
			return
		}
	}()

	if c, ok := conn.(*tls.Conn); ok {
		// we do this here so that we can get the SNI server name
		if err := c.Handshake(); err != nil {
			s.handleError(fmt.Errorf("tls handshake: %w", err))
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
			s.handleError(fmt.Errorf("reading message: %w", err))
			return
		}

		scope := m.Scope()

		handler, ok := s.registry.Get(scope)
		if !ok {
			s.handleError(fmt.Errorf("undefined scope: %+v", scope))
			return
		}

		go handler(ctx, conn, m)
	}
}
