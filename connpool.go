package mongonet

import "fmt"
import "net"
import "sync"
import "sync/atomic"
import "time"

type PooledConnection struct {
	conn         net.Conn
	lastUsedUnix int64
	pool         *ConnectionPool
	closed       bool
}

func (pc *PooledConnection) Close() {
	pc.pool.Put(pc)
}

// ---

type ConnectionPool struct {
	address        string
	timeoutSeconds int64
	trace          bool

	pool         []*PooledConnection
	poolMutex    sync.Mutex

	totalCreated int64
}

func NewConnectionPool(address string) *ConnectionPool {
	return &ConnectionPool{address, 3600, false, []*PooledConnection{}, sync.Mutex{}, 0}
}

func (cp *ConnectionPool) Trace(s string) {
	if cp.trace {
		fmt.Printf(s)
	}
}

func (cp *ConnectionPool) LoadTotalCreated() int64 {
	return atomic.LoadInt64(&cp.totalCreated)
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
	cp.Trace("ConnectionPool::Get\n")

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

	newConn, err := net.Dial("tcp", cp.address)
	if err != nil {
		return &PooledConnection{}, err
	}

	atomic.AddInt64(&cp.totalCreated, 1)
	return &PooledConnection{newConn, 0, cp, false}, nil
}

func (cp *ConnectionPool) Put(conn *PooledConnection) {
	cp.Trace("ConnectionPool::Put\n")
	if conn.closed {
		panic("closing a connection twice")
	}
	conn.lastUsedUnix = time.Now().Unix()
	conn.closed = true

	cp.poolMutex.Lock()
	defer cp.poolMutex.Unlock()
	cp.pool = append(cp.pool, conn)

}
