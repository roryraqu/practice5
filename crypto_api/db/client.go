package db

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
	"os"
)

const DefaultSchema = "schema.json"

var DBAddr = getDBAddress()

func getDBAddress() string {
	envAddr := os.Getenv("DB_ADDR")
	if envAddr != "" {
		return envAddr
	}
	return "localhost:7432"
}

type ConnectionPool struct {
	conns    chan net.Conn
	maxOpen  int
	mu       sync.Mutex
}

var pool *ConnectionPool

func InitConnection() error {
	pool = &ConnectionPool{
		conns:   make(chan net.Conn, 50),
		maxOpen: 50,
	}
	for i := 0; i < 5; i++ {
		c, err := createConn()
		if err == nil {
			pool.conns <- c
		}
	}
	return nil
}

func createConn() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", DBAddr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (p *ConnectionPool) Get() (net.Conn, error) {
	select {
	case conn := <-p.conns:
		return conn, nil
	default:
		return createConn()
	}
}

func (p *ConnectionPool) Put(conn net.Conn) {
	if conn == nil {
		return
	}
	select {
	case p.conns <- conn:
	default:
		conn.Close()
	}
}

func (p *ConnectionPool) Discard(conn net.Conn) {
	if conn != nil {
		conn.Close()
	}
}

func ExecQuery(query string) (string, error) {
	if pool == nil {
		if err := InitConnection(); err != nil {
			return "", err
		}
	}

	conn, err := pool.Get()
	if err != nil {
		return "", fmt.Errorf("pool error: %v", err)
	}

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	cmd := fmt.Sprintf("-s \"%s\" -q \"%s\"\n", DefaultSchema, query)
	_, err = conn.Write([]byte(cmd))
	if err != nil {
		pool.Discard(conn)
		conn, err = createConn()
		if err != nil { return "", err }
		conn.Write([]byte(cmd))
	}

	var responseBuffer bytes.Buffer
	tmp := make([]byte, 4096)
	
	complete := false
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := conn.Read(tmp)
		if err != nil {
			pool.Discard(conn)
			return "", err
		}
		
		chunk := tmp[:n]
		if idx := bytes.IndexByte(chunk, 4); idx != -1 {
			responseBuffer.Write(chunk[:idx])
			complete = true
			break
		} else {
			responseBuffer.Write(chunk)
		}
	}

	if complete {
		pool.Put(conn)
	} else {
		pool.Discard(conn)
	}

	response := responseBuffer.String()
	if strings.Contains(response, "Error") {
		return "", fmt.Errorf("DB error: %s", response)
	}

	return strings.TrimSpace(response), nil
}