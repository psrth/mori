package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/sqlite/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// Proxy is a pgwire-compatible proxy that executes queries against SQLite databases.
// Clients connect using PostgreSQL wire protocol; the proxy translates internally
// to SQLite operations via database/sql.
type Proxy struct {
	prodDSN    string
	shadowDSN  string
	prodDB     *sql.DB
	shadowDB   *sql.DB
	classifier core.Classifier
	router     *core.Router
	port       int
	verbose    bool

	deltaMap        *delta.Map
	tombstones      *delta.TombstoneSet
	tables          map[string]schema.TableMeta
	schemaRegistry  *coreSchema.Registry
	moriDir         string
	logger          *logging.Logger
	maxRowsHydrate  int

	listenerMu sync.Mutex
	listener   net.Listener

	activeConns sync.WaitGroup
	connCount   atomic.Int64
	shutdownCh  chan struct{}
	once        sync.Once
}

// New creates a SQLite Proxy.
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

// SetMaxRowsHydrate configures the maximum number of rows that can be hydrated
// from Prod in a single operation. 0 means unlimited.
func (p *Proxy) SetMaxRowsHydrate(n int) {
	p.maxRowsHydrate = n
}

// capSQL appends a LIMIT clause to cap rows fetched from Prod.
// If maxRowsHydrate is 0 or the outer query already has LIMIT, returns sql unchanged.
func (p *Proxy) capSQL(sql string) string {
	if p.maxRowsHydrate <= 0 {
		return sql
	}
	if hasOuterLimit(sql) {
		return sql
	}
	return sql + fmt.Sprintf(" LIMIT %d", p.maxRowsHydrate)
}

// hasOuterLimit reports whether sql contains a LIMIT keyword at the outermost
// level (not inside parenthesized subqueries). It handles any whitespace
// (spaces, tabs, newlines) around the keyword.
func hasOuterLimit(sql string) bool {
	upper := strings.ToUpper(sql)
	depth := 0
	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+5 <= len(upper) && upper[i:i+5] == "LIMIT" {
				before := i == 0 || upper[i-1] == ' ' || upper[i-1] == '\t' || upper[i-1] == '\n' || upper[i-1] == '\r' || upper[i-1] == ')'
				after := i+5 == len(upper) || upper[i+5] == ' ' || upper[i+5] == '\t' || upper[i+5] == '\n' || upper[i+5] == '\r'
				if before && after {
					return true
				}
			}
		}
	}
	return false
}

// ListenAndServe opens the SQLite databases, binds the TCP listener,
// and enters the accept loop.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	// Open prod database (read-only).
	var err error
	p.prodDB, err = sql.Open("sqlite", p.prodDSN)
	if err != nil {
		return fmt.Errorf("failed to open prod SQLite: %w", err)
	}
	if err := p.prodDB.Ping(); err != nil {
		return fmt.Errorf("failed to connect to prod SQLite: %w", err)
	}
	// Enable WAL mode and set read-only pragma.
	p.prodDB.Exec("PRAGMA journal_mode=WAL")
	p.prodDB.Exec("PRAGMA query_only=ON")

	// Open shadow database (read-write).
	if p.shadowDSN != "" {
		p.shadowDB, err = sql.Open("sqlite", p.shadowDSN)
		if err != nil {
			return fmt.Errorf("failed to open shadow SQLite: %w", err)
		}
		if err := p.shadowDB.Ping(); err != nil {
			return fmt.Errorf("failed to connect to shadow SQLite: %w", err)
		}
		p.shadowDB.Exec("PRAGMA journal_mode=WAL")
		p.shadowDB.Exec("PRAGMA foreign_keys=OFF")
	}

	// P1 2.5: Discover foreign keys from the prod database and register them.
	if p.schemaRegistry != nil && p.prodDB != nil {
		var tableNames []string
		for name := range p.tables {
			tableNames = append(tableNames, name)
		}
		if err := DetectForeignKeys(ctx, p.prodDB, p.schemaRegistry, tableNames); err != nil {
			if p.verbose {
				log.Printf("FK discovery warning: %v", err)
			}
		} else if p.verbose && len(tableNames) > 0 {
			log.Printf("FK discovery complete for %d tables", len(tableNames))
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
		log.Printf("Mori SQLite proxy listening on %s (pgwire) → prod=%s shadow=%s",
			ln.Addr().String(), p.prodDSN, p.shadowDSN)
	} else {
		log.Printf("Mori SQLite proxy listening on %s (pgwire) → %s (pass-through)",
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
		log.Println("All connections drained. SQLite proxy stopped.")
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
