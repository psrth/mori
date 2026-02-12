package proxy

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
)

// Proxy is a protocol-aware PostgreSQL proxy that classifies queries and
// routes them to either the production database or the local Shadow database.
// If shadowAddr is empty or classifier/router is nil, it operates in
// pass-through mode (all traffic goes to Prod).
type Proxy struct {
	prodAddr     string
	shadowAddr   string          // empty = Shadow unavailable
	shadowDBName string          // database name for Shadow startup
	classifier   core.Classifier // nil = pass-through mode
	router       *core.Router    // nil = pass-through mode
	port         int
	verbose      bool

	deltaMap        *delta.Map
	tombstones      *delta.TombstoneSet
	tables          map[string]schema.TableMeta
	schemaRegistry  *coreSchema.Registry
	moriDir         string

	listenerMu sync.Mutex
	listener   net.Listener

	activeConns sync.WaitGroup
	connCount   atomic.Int64
	shutdownCh  chan struct{}
	once        sync.Once
}

// New creates a Proxy. If shadowAddr is empty or classifier/router is nil,
// the proxy operates in pass-through mode (all traffic goes to Prod).
// deltaMap, tombstones, tables, and moriDir enable write-path tracking;
// pass nil/empty to disable.
func New(prodAddr, shadowAddr, shadowDBName string, listenPort int, verbose bool,
	classifier core.Classifier, router *core.Router,
	deltaMap *delta.Map, tombstones *delta.TombstoneSet,
	tables map[string]schema.TableMeta, moriDir string,
	schemaRegistry *coreSchema.Registry,
) *Proxy {
	return &Proxy{
		prodAddr:       prodAddr,
		shadowAddr:     shadowAddr,
		shadowDBName:   shadowDBName,
		classifier:     classifier,
		router:         router,
		port:           listenPort,
		verbose:        verbose,
		deltaMap:       deltaMap,
		tombstones:     tombstones,
		tables:         tables,
		schemaRegistry: schemaRegistry,
		moriDir:        moriDir,
		shutdownCh:     make(chan struct{}),
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
	p.listenerMu.Lock()
	p.listener = ln
	p.listenerMu.Unlock()

	if p.canRoute() {
		log.Printf("Mori proxy listening on %s → prod=%s shadow=%s", ln.Addr().String(), p.prodAddr, p.shadowAddr)
	} else {
		log.Printf("Mori proxy listening on %s → %s (pass-through)", ln.Addr().String(), p.prodAddr)
	}

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

	p.listenerMu.Lock()
	ln := p.listener
	p.listenerMu.Unlock()
	if ln != nil {
		ln.Close()
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
	p.listenerMu.Lock()
	ln := p.listener
	p.listenerMu.Unlock()
	if ln == nil {
		return ""
	}
	return ln.Addr().String()
}

// canRoute reports whether the proxy has all dependencies for query routing.
func (p *Proxy) canRoute() bool {
	return p.shadowAddr != "" && p.classifier != nil && p.router != nil
}
