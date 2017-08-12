package mongonet

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/logging"
	"github.com/mongodb/grip/send"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2/bson"
)

type Proxy struct {
	config   ProxyConfig
	connPool *ConnectionPool
	logger   grip.Journaler
}

type ProxySession struct {
	proxy       *Proxy
	conn        io.ReadWriteCloser
	remoteAddr  net.Addr
	interceptor ProxyInterceptor
	logger      grip.Journaler

	SSLServerName string
}

type MongoError struct {
	err      error
	code     int
	codeName string
}

func NewMongoError(err error, code int, codeName string) MongoError {
	return MongoError{err, code, codeName}
}

func (me MongoError) ToBSON() bson.D {
	doc := bson.D{{"ok", 0}}

	if me.err != nil {
		doc = append(doc, bson.DocElem{"errmsg", me.err.Error()})
	}

	doc = append(doc,
		bson.DocElem{"code", me.code},
		bson.DocElem{"codeName", me.codeName})

	return doc
}

func (me MongoError) Error() string {
	return fmt.Sprintf(
		"code=%v codeName=%v errmsg = %v",
		me.code,
		me.codeName,
		me.err.Error(),
	)
}

type ResponseInterceptor interface {
	InterceptMongoToClient(m Message) (Message, error)
}

type ProxyInterceptor interface {
	InterceptClientToMongo(m Message) (Message, ResponseInterceptor, error)
	Close()
	TrackRequest(MessageHeader)
	TrackResponse(MessageHeader)
	CheckConnection() error
	CheckConnectionInterval() time.Duration
}

type ProxyInterceptorFactory interface {
	// This has to be thread safe, will be called from many clients
	NewInterceptor(ps *ProxySession) (ProxyInterceptor, error)
}

// -----

func (ps *ProxySession) RemoteAddr() net.Addr {
	return ps.remoteAddr
}

func (ps *ProxySession) GetLogger() grip.Journaler {
	return ps.logger
}

func (ps *ProxySession) ServerPort() int {
	return ps.proxy.config.BindPort
}

func (ps *ProxySession) Stats() bson.D {
	return bson.D{{"connectionPool", bson.D{{"totalCreated", ps.proxy.connPool.totalCreated}}}}
}

func (ps *ProxySession) RespondToCommand(clientMessage Message, doc SimpleBSON) error {
	if clientMessage.Header().OpCode == OP_QUERY {
		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},
			0, // flags - error bit
			0, // cursor id
			0, // StartingFrom
			1, // NumberReturned
			[]SimpleBSON{doc},
		}
		return SendMessage(rm, ps.conn)
	} else if clientMessage.Header().OpCode == OP_COMMAND {
		rm := &CommandReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_COMMAND_REPLY},
			doc,
			SimpleBSONEmpty(),
			[]SimpleBSON{},
		}
		return SendMessage(rm, ps.conn)
	} else {
		panic("impossible")
	}

}

func (ps *ProxySession) respondWithError(clientMessage Message, err error) error {
	ps.logger.Info(errors.Wrap(err, "respondWithError"))

	var errBSON bson.D
	if err == nil {
		errBSON = bson.D{{"ok", 1}}
	} else if mongoErr, ok := err.(MongoError); ok {
		errBSON = mongoErr.ToBSON()
	} else {
		errBSON = bson.D{{"ok", 0}, {"errmsg", err.Error()}}
	}

	switch clientMessage.Header().OpCode {
	case OP_QUERY, OP_GET_MORE:
		doc, myErr := SimpleBSONConvert(errBSON)
		if myErr != nil {
			return myErr
		}

		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},

			// We should not set the error bit because we are
			// responding with errmsg instead of $err
			0, // flags - error bit

			0, // cursor id
			0, // StartingFrom
			1, // NumberReturned
			[]SimpleBSON{doc},
		}
		return SendMessage(rm, ps.conn)
	case OP_COMMAND:
		doc, myErr := SimpleBSONConvert(errBSON)
		if myErr != nil {
			return myErr
		}

		rm := &CommandReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_COMMAND_REPLY},
			doc,
			SimpleBSONEmpty(),
			[]SimpleBSON{},
		}
		return SendMessage(rm, ps.conn)
	default:
		panic("impossible")
	}

}

func (ps *ProxySession) doLoop(pooledConn *PooledConnection) (*PooledConnection, error) {
	m, err := ReadMessage(ps.conn)
	if err != nil {
		if err == io.EOF {
			return pooledConn, err
		}
		return pooledConn, errors.Wrap(err, "got error reading from client")
	}

	var respInter ResponseInterceptor
	if ps.interceptor != nil {
		ps.interceptor.TrackRequest(m.Header())

		m, respInter, err = ps.interceptor.InterceptClientToMongo(m)
		if err != nil {
			if m == nil {
				if pooledConn != nil {
					pooledConn.Close()
				}
				return nil, err
			}
			if !m.HasResponse() {
				// we can't respond, so we just fail
				return pooledConn, err
			}
			err = ps.respondWithError(m, err)
			if err != nil {
				return pooledConn, errors.Wrap(err, "couldn't send error response to client")
			}
			return pooledConn, nil
		}
		if m == nil {
			// already responded
			return pooledConn, nil
		}
	}

	if pooledConn == nil {
		pooledConn, err = ps.proxy.connPool.Get()
		if err != nil {
			return nil, errors.Wrap(err, "cannot get connection to mongo")
		}
	}

	if pooledConn.closed {
		panic("oh no!")
	}
	mongoConn := pooledConn.conn

	err = SendMessage(m, mongoConn)
	if err != nil {
		return nil, errors.Wrap(err, "error writing to mongo")
	}

	if !m.HasResponse() {
		return pooledConn, nil
	}

	defer pooledConn.Close()

	inExhaustMode :=
		m.Header().OpCode == OP_QUERY &&
			m.(*QueryMessage).Flags&(1<<6) != 0

	for {
		resp, err := ReadMessage(mongoConn)
		if err != nil {
			pooledConn.bad = true
			return nil, errors.Wrap(err, "got error reading response from mongo")
		}

		if respInter != nil {
			resp, err = respInter.InterceptMongoToClient(resp)
			if err != nil {
				return nil, errors.Wrap(err, "error intercepting message")
			}
		}

		err = SendMessage(resp, ps.conn)
		if err != nil {
			return nil, errors.Wrap(err, "got error sending response to client")
		}

		if ps.interceptor != nil {
			ps.interceptor.TrackResponse(resp.Header())
		}

		if !inExhaustMode {
			return nil, nil
		}

		if resp.(*ReplyMessage).CursorId == 0 {
			return nil, nil
		}
	}
}

func (ps *ProxySession) Run(conn net.Conn) {
	var err error
	defer conn.Close()

	switch c := conn.(type) {
	case *tls.Conn:
		// we do this here so that we can get the SNI server name
		err = c.Handshake()
		if err != nil {
			ps.logger.Warning(errors.Wrap(err, "error doing tls handshake"))
			return
		}
		ps.SSLServerName = c.ConnectionState().ServerName
	}

	ps.logger.Infof("new connection SSLServerName [%s]", ps.SSLServerName)

	if ps.proxy.config.InterceptorFactory != nil {
		ps.interceptor, err = ps.proxy.config.InterceptorFactory.NewInterceptor(ps)
		if err != nil {
			ps.logger.Info(errors.Wrap(err, "error creating new interceptor because"))
			return
		}
		defer ps.interceptor.Close()

		ps.conn = CheckedConn{conn, ps.interceptor}
	}

	defer ps.logger.Info("socket closed")

	var pooledConn *PooledConnection

	for {
		pooledConn, err = ps.doLoop(pooledConn)
		if err != nil {
			if pooledConn != nil {
				pooledConn.Close()
			}
			if err != io.EOF {
				ps.logger.Warning(errors.Wrap(err, "error doing loop"))
			}
			return
		}
	}

	if pooledConn != nil {
		pooledConn.Close()
	}
}

// -------

func NewProxy(pc ProxyConfig) Proxy {
	p := Proxy{
		config:   pc,
		connPool: NewConnectionPool(pc.MongoAddress(), pc.MongoSSL, pc.MongoRootCAs, pc.MongoSSLSkipVerify, pc.ConnectionPoolHook),
		logger:   &logging.Grip{send.MakeNative()},
	}

	p.logger.SetName("proxy")

	return p
}

func (p *Proxy) Run() error {
	bindTo := fmt.Sprintf("%s:%d", p.config.BindHost, p.config.BindPort)
	p.logger.Warningf("listening on %s", bindTo)

	var tlsConfig *tls.Config

	if p.config.UseSSL {
		if len(p.config.SSLKeys) == 0 {
			return fmt.Errorf("no ssl keys configured")
		}

		certs := []tls.Certificate{}
		for _, pair := range p.config.SSLKeys {
			cer, err := tls.LoadX509KeyPair(pair.CertFile, pair.KeyFile)
			if err != nil {
				return errors.Wrapf(err, "cannot LoadX509KeyPair from %s %s", pair.CertFile, pair.KeyFile)
			}
			certs = append(certs, cer)
		}

		tlsConfig = &tls.Config{Certificates: certs}

		tlsConfig.BuildNameToCertificate()
	}

	ln, err := net.Listen("tcp", bindTo)
	if err != nil {
		return errors.Wrap(err, "cannot start listening in proxy")
	}

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return errors.Wrap(err, "could not accept in proxy")
		}

		if p.config.TCPKeepAlivePeriod > 0 {
			switch conn := conn.(type) {
			case *net.TCPConn:
				conn.SetKeepAlive(true)
				conn.SetKeepAlivePeriod(p.config.TCPKeepAlivePeriod)
			default:
				p.logger.Warningf("Want to set TCP keep alive on accepted connection but connection is not *net.TCPConn. It is %T", conn)
			}
		}

		if p.config.UseSSL {
			conn = tls.Server(conn, tlsConfig)
		}

		logger := &logging.Grip{send.MakeNative()}
		remoteAddr := conn.RemoteAddr()
		logger.SetName(fmt.Sprintf("ProxySession %s", remoteAddr))
		c := &ProxySession{
			proxy:      p,
			remoteAddr: remoteAddr,
			logger:     logger,
		}

		go c.Run(conn)
	}
}
