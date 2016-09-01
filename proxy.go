package mongonet

import "crypto/tls"
import "fmt"
import "io"
import "net"

import "gopkg.in/mgo.v2/bson"

import "github.com/mongodb/slogger/v2/slogger"

type Proxy struct {
	config ProxyConfig

	logger *slogger.Logger
}

type ProxySession struct {
	proxy *Proxy
	conn  net.Conn

	interceptor ProxyInterceptor

	logger *slogger.Logger

	SSLServerName string
}

type ResponseInterceptor interface {
	InterceptMongoToClient(m Message) (Message, error)
}

type ProxyInterceptor interface {
	InterceptClientToMongo(m Message) (Message, ResponseInterceptor, error)
	Close()
}

type ProxyInterceptorFactory interface {
	// This has to be thread safe, will be called from many clients
	NewInterceptor(ps *ProxySession) (ProxyInterceptor, error)
}

// -----

func (ps *ProxySession) GetLogger() *slogger.Logger {
	return ps.logger
}

func (ps *ProxySession) ServerPort() int {
     return ps.proxy.config.BindPort
}

func (ps *ProxySession) xferMongoToClient(mongoConn net.Conn, respInter ResponseInterceptor) (Message, error) {
	resp, err := ReadMessage(mongoConn)
	if err != nil {
		return resp, NewStackErrorf("got error reading response from mongo %s", err)
	}

	if respInter != nil {
		resp, err = respInter.InterceptMongoToClient(resp)
		if err != nil {
			return nil, NewStackErrorf("error intercepting message %s", err)
		}
	}

	err = SendMessage(resp, ps.conn)
	if err != nil {
		return resp, NewStackErrorf("got error sending response to client %s", err)
	}

	return resp, nil
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
	ps.logger.Logf(slogger.INFO, "respondWithError %s", err)

	if clientMessage.Header().OpCode == OP_QUERY {
		errDoc := bson.D{{"$err", err.Error()}, {"ok", 0}}
		doc, myErr := SimpleBSONConvert(errDoc)
		if myErr != nil {
			return myErr
		}

		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},
			2, // flags - error bit
			0, // cursor id
			0, // StartingFrom
			1, // NumberReturned
			[]SimpleBSON{doc},
		}
		return SendMessage(rm, ps.conn)
	} else if clientMessage.Header().OpCode == OP_COMMAND {
		errDoc := bson.D{{"errmsg", err.Error()}, {"ok", 0}}
		doc, myErr := SimpleBSONConvert(errDoc)
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
	} else {
		panic("impossible")
	}

}

func (ps *ProxySession) doLoop(mongoConn net.Conn) error {
	m, err := ReadMessage(ps.conn)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return NewStackErrorf("got error reading from client: %s", err)
	}

	var respInter ResponseInterceptor
	if ps.interceptor != nil {
		m, respInter, err = ps.interceptor.InterceptClientToMongo(m)
		if err != nil {
			if !m.HasResponse() {
				// we can't respond, so we just fail
				return err
			}
			err = ps.respondWithError(m, err)
			if err != nil {
				return NewStackErrorf("couldn't send error response to client %s", err)
			}
			return nil
		}
		if m == nil {
			// already responded
			return nil
		}
	}

	err = SendMessage(m, mongoConn)
	if err != nil {
		return NewStackErrorf("error writing to mongo: %s", err)
	}

	if !m.HasResponse() {
		return nil
	}

	inExhaustMode :=
		m.Header().OpCode == OP_QUERY &&
			m.(*QueryMessage).Flags&(1<<6) != 0

	for {
		resp, err := ps.xferMongoToClient(mongoConn, respInter)
		if err != nil {
			return err
		}

		if !inExhaustMode {
			return nil
		}

		if resp.(*ReplyMessage).CursorId == 0 {
			return nil
		}
	}
}

func (ps *ProxySession) Run() {
	var err error
	defer ps.conn.Close()

	switch c := ps.conn.(type) {
	case *tls.Conn:
		// we do this here so that we can get the SNI server name
		err = c.Handshake()
		if err != nil {
			ps.logger.Logf(slogger.ERROR, "error doing tls handshake %s", err)
			return
		}
		ps.SSLServerName = c.ConnectionState().ServerName
	}

	ps.logger.Logf(slogger.INFO, "new connection SSLServerName [%s]", ps.SSLServerName)

	if ps.proxy.config.InterceptorFactory != nil {
		ps.interceptor, err = ps.proxy.config.InterceptorFactory.NewInterceptor(ps)
		if err != nil {
			ps.logger.Logf(slogger.INFO, "error creating new interceptor %s", err)
			return
		}
		defer ps.interceptor.Close()
	}

	// TODO: eventually this gets pooled
	mongoConn, err := net.Dial("tcp", ps.proxy.config.MongoAddress())
	if err != nil {
		ps.logger.Logf(slogger.ERROR, "error connecting to mongo: %s %s\n", ps.proxy.config.MongoAddress(), err)
		return
	}

	defer mongoConn.Close()

	defer ps.logger.Logf(slogger.INFO, "socket closed")

	for {
		err = ps.doLoop(mongoConn)
		if err == io.EOF {
			return
		}
		if err != nil {
			ps.logger.Logf(slogger.WARN, "error doing loop: %s", err)
			return
		}
	}

}

// -------

func NewProxy(pc ProxyConfig) Proxy {
	p := Proxy{pc, nil}

	p.logger = p.NewLogger("proxy")

	return p
}

func (p *Proxy) NewLogger(prefix string) *slogger.Logger {

	level := slogger.INFO
	if p.config.VerboseLevel == 1 {
		level = slogger.DEBUG
	} else if p.config.VerboseLevel == 2 {
		level = slogger.OFF
	}

	filters := []slogger.TurboFilter{slogger.TurboLevelFilter(level)}

	return &slogger.Logger{prefix, []slogger.Appender{slogger.StdOutAppender()}, 0, filters}
}

func (p *Proxy) Run() error {

	bindTo := fmt.Sprintf("%s:%d", p.config.BindHost, p.config.BindPort)
	p.logger.Logf(slogger.WARN, "listening on %s", bindTo)

	var err error
	var ln net.Listener

	if p.config.UseSSL {
		if len(p.config.SSLKeys) == 0 {
			return fmt.Errorf("no ssl keys configured")
		}

		certs := []tls.Certificate{}
		for _, pair := range p.config.SSLKeys {
			cer, err := tls.LoadX509KeyPair(pair.CertFile, pair.KeyFile)
			if err != nil {
				return fmt.Errorf("cannot LoadX509KeyPair from %s %s %s", pair.CertFile, pair.KeyFile, err)
			}
			certs = append(certs, cer)
		}

		config := &tls.Config{Certificates: certs}

		config.BuildNameToCertificate()

		ln, err = tls.Listen("tcp", bindTo, config)
		if err != nil {
			return fmt.Errorf("cannot start listen tls in proxy: %s", err)
		}

	} else {
		ln, err = net.Listen("tcp", bindTo)
		if err != nil {
			return NewStackErrorf("cannot start listening in proxy: %s", err)
		}
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return NewStackErrorf("could not accept in proxy: %s", err)
		}

		c := &ProxySession{p, conn, nil, p.NewLogger(fmt.Sprintf("ProxySession %s", conn.RemoteAddr())), ""}
		go c.Run()
	}

}
