package mongonet

import "fmt"
import "net"
import "testing"
import "time"

type FakeServer struct {
	numAccepted int
}

func (fs *FakeServer) doThread(conn net.Conn, threadNumber int) {
	defer conn.Close()
}

func (fs *FakeServer) run(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		fs.numAccepted++
		go fs.doThread(conn, fs.numAccepted)
	}
}

func (fs *FakeServer) start(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	go fs.run(ln)
	return nil
}

func fun(cp *ConnectionPool) error {
	conn, err := cp.Get()
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}

func TestConnectionPool1(t *testing.T) {
	port := 12349
	fs := FakeServer{}
	err := fs.start(port)
	if err != nil {
		t.Errorf("can't start %s", err)
	}

	cp := NewConnectionPool(fmt.Sprintf("127.0.0.1:%d", port))
	cp.timeoutSeconds = 1

	err = fun(cp)
	if err != nil {
		t.Errorf("error funning %s", err)
		return
	}

	if cp.totalCreated != 1 {
		t.Errorf("why is total created %d", cp.totalCreated)
		return
	}

	err = fun(cp)
	if err != nil {
		t.Errorf("error funning %s", err)
		return
	}

	if cp.totalCreated != 1 {
		t.Errorf("why is total created %d", cp.totalCreated)
		return
	}

	garbage, err := cp.Get()
	if err != nil {
		panic(err)
	}
	defer garbage.Close()

	err = fun(cp)
	if err != nil {
		t.Errorf("error funning %s", err)
		return
	}

	if cp.totalCreated != 2 {
		t.Errorf("why is total created %d", cp.totalCreated)
		return
	}

	time.Sleep(time.Duration(int64(time.Second) * (cp.timeoutSeconds + 1)))

	err = fun(cp)
	if err != nil {
		t.Errorf("error funning %s", err)
		return
	}

	if cp.totalCreated != 3 {
		t.Errorf("why is total created %d", cp.totalCreated)
		return
	}

}
