package mongorpc

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"

	"github.com/mongodb/grip"
	"github.com/pkg/errors"
	"github.com/tychoish/mongorpc/mongowire"
	"golang.org/x/net/context"
)

type HandlerFunc func(context.Context, io.Writer, mongowire.Message)

type Service struct {
	addr     string
	registry *OperationRegistry
}

func NewService(listenAddr string, port int) *Service {
	s := &Service{
		addr:     fmt.Sprintf("%s:%d", listenAddr, port),
		registry: &OperationRegistry{ops: make(map[mongowire.OpScope]HandlerFunc)},
	}

}

func (s *Service) RegisterOperation(scope *mongowire.OpScope) error {
	if err := scope.Validate(); err != nil {
		return errors.Wrap(err, "scope does not validate")
	}

	if err := s.registry.Add(scope); err != nil {
		return errors.Wrap(err, "problem adding scope to registry")
	}

	return nil
}

func (s *Service) Run(ctx context.Context) error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return errors.Wrapf(err, "problem listening on %s", s.addr)
	}
	defer l.Close()

	grip.Infof("listening for connections on %s", s.addr)

	for {
		conn, err := l.Accept()
		grip.Warning(errors.Wrap(err, "problem accepting connection"))

		go s.dispatchRequest(ctx, conn)
	}
}

func (s *Service) dispatchRequest(ctx context.Context, conn net.Conn) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	defer conn.Close()

	if c, ok := conn.(*tls.Conn); ok {
		// we do this here so that we can get the SNI server name
		if err := c.Handshake(); err != nil {
			grip.Warning(errors.Wrap(err, "error doing tls handshake"))
			return
		}
		// SSLServerName = c.ConnectionState().ServerName
	}

	m, err := mongowire.ReadMessage(conn)
	if err != nil {
		if err == io.EOF {
			return
		}
		grip.Warning("problem reading message")
	}

	scope := m.Scope()

	handler, ok := s.registry.Get(scope)
	if !ok {
		grip.Warning("undefined command scope: %+v", scope)
		return
	}

	handler(ctx, conn, m)
}
