package mongonet

import (
	"crypto/tls"
	"net"
	"time"
)

type ConnChecker interface {
	CheckConnection() error
	CheckConnectionInterval() time.Duration // set to 0 to not check
}

type CheckedConn struct {
	conn    net.Conn
	checker ConnChecker
}

func (c CheckedConn) Read(b []byte) (n int, err error) {
	for {
		interval := c.checker.CheckConnectionInterval()
		if interval > 0 {
			if err = c.checker.CheckConnection(); err != nil {
				return n, err
			}

			if err = c.conn.SetReadDeadline(time.Now().Add(interval)); err != nil {
				return n, err
			}
		}

		nDelta, err := c.conn.Read(b[n:])
		n += nDelta

		// If a timeout occurs, the TLS connection will be corrupted, and all future writes
		// will return the same error. (https://golang.org/pkg/crypto/tls/#Conn.SetDeadline)
		// Therefore, always return.
		if _, ok := c.conn.(*tls.Conn); ok {
			return n, err
		}
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			return n, err
		}
	}
}

func (c CheckedConn) Write(b []byte) (n int, err error) {
	for {
		interval := c.checker.CheckConnectionInterval()
		if interval > 0 {
			if err = c.checker.CheckConnection(); err != nil {
				return n, err
			}

			if err = c.conn.SetWriteDeadline(time.Now().Add(interval)); err != nil {
				return n, err
			}
		}

		nDelta, err := c.conn.Write(b[n:])
		n += nDelta

		// If a timeout occurs, the TLS connection will be corrupted, and all future writes
		// will return the same error. (https://golang.org/pkg/crypto/tls/#Conn.SetDeadline)
		// Therefore, always return.
		if _, ok := c.conn.(*tls.Conn); ok {
			return n, err
		}
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			return n, err
		}
	}
}

func (c CheckedConn) Close() error {
	return c.conn.Close()
}
