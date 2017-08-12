package mongonet

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mongodb/grip"
)

type PooledConnection struct {
	conn         net.Conn
	lastUsedUnix int64
	pool         *ConnectionPool
	closed       bool
	bad          bool
}

func (pc *PooledConnection) Close() {
	pc.pool.Put(pc)
}

// ---

type ConnectionHook func(net.Conn) error

type ConnectionPool struct {
	address        string
	ssl            bool
	rootCAs        *x509.CertPool
	sslSkipVerify  bool
	timeoutSeconds int64
	trace          bool

	pool      []*PooledConnection
	poolMutex sync.Mutex

	totalCreated int64

	postCreateHook ConnectionHook
}

func NewConnectionPool(address string, ssl bool, rootCAs *x509.CertPool, sslSkipVerify bool, hook ConnectionHook) *ConnectionPool {
	return &ConnectionPool{
		address:        address,
		ssl:            ssl,
		rootCAs:        rootCAs,
		sslSkipVerify:  sslSkipVerify,
		timeoutSeconds: 3600,
		pool:           []*PooledConnection{},
		postCreateHook: hook,
	}
}

func (cp *ConnectionPool) LoadTotalCreated() int64 {
	return atomic.LoadInt64(&cp.totalCreated)
}

func (cp *ConnectionPool) CurrentInPool() int {
	cp.poolMutex.Lock()
	defer cp.poolMutex.Unlock()
	return len(cp.pool)
}

func (cp *ConnectionPool) rawGet() *PooledConnection {
	cp.poolMutex.Lock()
	defer cp.poolMutex.Unlock()

	last := len(cp.pool) - 1
	if last < 0 {
		return nil
	}

	ret := cp.pool[last]
	cp.pool = cp.pool[:last]

	return ret
}

func (cp *ConnectionPool) Get() (*PooledConnection, error) {
	grip.Debug("ConnectionPool::Get")

	for {
		conn := cp.rawGet()
		if conn == nil {
			break
		}

		// if a connection has been idle for more than an hour, don't re-use it
		if time.Now().Unix()-conn.lastUsedUnix < cp.timeoutSeconds {
			conn.closed = false
			return conn, nil
		}
		// close it since we're not going to use it anymore
		conn.conn.Close()
	}

	var err error
	var newConn net.Conn

	if cp.ssl {
		tlsConfig := &tls.Config{RootCAs: cp.rootCAs, InsecureSkipVerify: cp.sslSkipVerify}
		newConn, err = tls.Dial("tcp", cp.address, tlsConfig)
	} else {
		newConn, err = net.Dial("tcp", cp.address)
	}

	if err != nil {
		return &PooledConnection{}, err
	}

	if cp.postCreateHook != nil {
		err = cp.postCreateHook(newConn)
		if err != nil {
			newConn.Close()
			return &PooledConnection{}, err
		}
	}

	atomic.AddInt64(&cp.totalCreated, 1)
	return &PooledConnection{newConn, 0, cp, false, false}, nil
}

func (cp *ConnectionPool) Put(conn *PooledConnection) {
	grip.Debug("ConnectionPool::Put")

	if conn.closed {
		panic("closing a connection twice")
	}

	conn.lastUsedUnix = time.Now().Unix()
	conn.closed = true

	if conn.bad {
		conn.conn.Close()
		return
	}

	cp.poolMutex.Lock()
	defer cp.poolMutex.Unlock()

	cp.pool = append(cp.pool, conn)
}
