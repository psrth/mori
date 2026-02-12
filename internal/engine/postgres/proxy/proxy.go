package proxy

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// Proxy is a transparent TCP relay between PostgreSQL clients and a
// production database. It accepts connections on a local port and
// forwards all bytes to prodAddr without modification.
type Proxy struct {
	prodAddr string
	listener net.Listener
	port     int
	verbose  bool

	activeConns sync.WaitGroup
	connCount   atomic.Int64
	shutdownCh  chan struct{}
	once        sync.Once
}

// New creates a Proxy that will relay connections to prodAddr.
// prodAddr must be in "host:port" format.
func New(prodAddr string, listenPort int, verbose bool) *Proxy {
	return &Proxy{
		prodAddr:   prodAddr,
		port:       listenPort,
		verbose:    verbose,
		shutdownCh: make(chan struct{}),
	}
}

// ListenAndServe binds the TCP listener and enters the accept loop.
// It blocks until the context is cancelled or Shutdown is called.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	addr := fmt.Sprintf("127.0.0.1:%d", p.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	p.listener = ln
	log.Printf("Mori proxy listening on %s → %s", ln.Addr().String(), p.prodAddr)

	// Close the listener when context is done or shutdown is signalled.
	go func() {
		select {
		case <-ctx.Done():
		case <-p.shutdownCh:
		}
		ln.Close()
	}()

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			select {
			case <-p.shutdownCh:
				return nil
			case <-ctx.Done():
				return nil
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		p.activeConns.Add(1)
		connID := p.connCount.Add(1)
		go p.handleConn(clientConn, connID)
	}
}

// Shutdown initiates graceful shutdown. It stops accepting new
// connections and waits for active connections to drain.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.once.Do(func() { close(p.shutdownCh) })

	if p.listener != nil {
		p.listener.Close()
	}

	done := make(chan struct{})
	go func() {
		p.activeConns.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All connections drained. Proxy stopped.")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Addr returns the listener's address, or "" if not yet listening.
func (p *Proxy) Addr() string {
	if p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}
