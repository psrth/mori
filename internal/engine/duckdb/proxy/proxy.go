package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/duckdb/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// Proxy is a pgwire-compatible proxy that executes queries against DuckDB databases.
// Clients connect using PostgreSQL wire protocol; the proxy translates internally
// to DuckDB operations via database/sql.
type Proxy struct {
	prodDSN    string
	shadowDSN  string
	prodDB     *sql.DB
	shadowDB   *sql.DB
	classifier core.Classifier
	router     *core.Router
	port       int
	verbose    bool

	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	moriDir        string
	logger         *logging.Logger

	listenerMu sync.Mutex
	listener   net.Listener

	activeConns sync.WaitGroup
	connCount   atomic.Int64
	shutdownCh  chan struct{}
	once        sync.Once
}

// New creates a DuckDB Proxy.
func New(prodDSN, shadowDSN string, listenPort int, verbose bool,
	classifier core.Classifier, router *core.Router,
	deltaMap *delta.Map, tombstones *delta.TombstoneSet,
	tables map[string]schema.TableMeta, moriDir string,
	schemaRegistry *coreSchema.Registry,
	logger *logging.Logger,
) *Proxy {
	return &Proxy{
		prodDSN:        prodDSN,
		shadowDSN:      shadowDSN,
		classifier:     classifier,
		router:         router,
		port:           listenPort,
		verbose:        verbose,
		deltaMap:       deltaMap,
		tombstones:     tombstones,
		tables:         tables,
		schemaRegistry: schemaRegistry,
		moriDir:        moriDir,
		logger:         logger,
		shutdownCh:     make(chan struct{}),
	}
}

// ListenAndServe opens the DuckDB databases, binds the TCP listener,
// and enters the accept loop.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	// Open prod database (read-only).
	var err error
	p.prodDB, err = sql.Open("duckdb", p.prodDSN+"?access_mode=READ_ONLY")
	if err != nil {
		return fmt.Errorf("failed to open prod DuckDB: %w", err)
	}
	if err := p.prodDB.Ping(); err != nil {
		return fmt.Errorf("failed to connect to prod DuckDB: %w", err)
	}

	// Open shadow database (read-write).
	if p.shadowDSN != "" {
		p.shadowDB, err = sql.Open("duckdb", p.shadowDSN)
		if err != nil {
			return fmt.Errorf("failed to open shadow DuckDB: %w", err)
		}
		if err := p.shadowDB.Ping(); err != nil {
			return fmt.Errorf("failed to connect to shadow DuckDB: %w", err)
		}
	}

	addr := fmt.Sprintf("127.0.0.1:%d", p.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	p.listenerMu.Lock()
	p.listener = ln
	p.listenerMu.Unlock()

	if p.canRoute() {
		log.Printf("Mori DuckDB proxy listening on %s (pgwire) → prod=%s shadow=%s",
			ln.Addr().String(), p.prodDSN, p.shadowDSN)
	} else {
		log.Printf("Mori DuckDB proxy listening on %s (pgwire) → %s (pass-through)",
			ln.Addr().String(), p.prodDSN)
	}

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

// Shutdown initiates graceful shutdown.
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
		log.Println("All connections drained. DuckDB proxy stopped.")
	case <-ctx.Done():
	}

	// Close database connections.
	if p.prodDB != nil {
		p.prodDB.Close()
	}
	if p.shadowDB != nil {
		p.shadowDB.Close()
	}
	p.logger.Close()
	return nil
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
	return p.shadowDB != nil && p.classifier != nil && p.router != nil
}
