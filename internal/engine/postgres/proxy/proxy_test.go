package proxy

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// startEchoServer creates a TCP server that echoes all received bytes back.
func startEchoServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				io.Copy(c, c)
				c.Close()
			}(conn)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

// waitForListener polls until the proxy's listener is ready.
func waitForListener(t *testing.T, p *Proxy, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatal("proxy listener did not start in time")
		default:
			if addr := p.Addr(); addr != "" {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestProxyRelay_RefusesWithoutShadow(t *testing.T) {
	echoAddr, cleanup := startEchoServer(t)
	defer cleanup()

	// Without shadow/classifier/router, the write guard should refuse connections.
	p := New(echoAddr, "", "", 0, false, nil, nil, nil, nil, nil, "", nil, nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- p.ListenAndServe(ctx) }()
	waitForListener(t, p, 2*time.Second)

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// The proxy should send an error response and close the connection.
	buf := make([]byte, 256)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil && n == 0 {
		t.Fatal(err)
	}
	// The response should start with 'E' (ErrorResponse).
	if n > 0 && buf[0] != 'E' {
		t.Errorf("expected ErrorResponse ('E'), got %q", buf[0])
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Errorf("ListenAndServe returned error: %v", err)
	}
}

func TestProxyProdUnreachable(t *testing.T) {
	// Point proxy at a port that's not listening.
	p := New("127.0.0.1:1", "", "", 0, false, nil, nil, nil, nil, nil, "", nil, nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.ListenAndServe(ctx)
	waitForListener(t, p, 2*time.Second)

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}

	// The proxy should close the client connection after failing to dial Prod.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Error("expected error reading from client after prod unreachable")
	}

	cancel()
}

func TestProxyShutdownDrains(t *testing.T) {
	// Use a slow echo server that holds connections.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				// Hold connection for a bit, then echo.
				time.Sleep(200 * time.Millisecond)
				io.Copy(c, c)
				c.Close()
			}(conn)
		}
	}()

	p := New(ln.Addr().String(), "", "", 0, false, nil, nil, nil, nil, nil, "", nil, nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.ListenAndServe(ctx)
	waitForListener(t, p, 2*time.Second)

	// Open a connection to keep the proxy busy.
	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}

	// Initiate shutdown — should wait for the connection.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	done := make(chan error, 1)
	go func() { done <- p.Shutdown(shutdownCtx) }()

	// Close the client connection to let the proxy drain.
	conn.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Shutdown returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete in time")
	}
}

func TestProxyConcurrentConns_RefusesWithoutShadow(t *testing.T) {
	echoAddr, cleanup := startEchoServer(t)
	defer cleanup()

	// Without shadow, all connections should be refused with an error response.
	p := New(echoAddr, "", "", 0, false, nil, nil, nil, nil, nil, "", nil, nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.ListenAndServe(ctx)
	waitForListener(t, p, 2*time.Second)

	const numConns = 10
	var wg sync.WaitGroup
	wg.Add(numConns)

	for i := 0; i < numConns; i++ {
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", p.Addr())
			if err != nil {
				t.Errorf("conn %d: dial error: %v", id, err)
				return
			}
			defer conn.Close()

			// Each connection should receive an error response.
			buf := make([]byte, 256)
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, err := conn.Read(buf)
			if err != nil && n == 0 {
				t.Errorf("conn %d: read error: %v", id, err)
				return
			}
			if n > 0 && buf[0] != 'E' {
				t.Errorf("conn %d: expected ErrorResponse ('E'), got %q", id, buf[0])
			}
		}(i)
	}

	wg.Wait()
	cancel()
}
