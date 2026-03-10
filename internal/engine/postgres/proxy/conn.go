package proxy

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/core/tlsutil"
)

// handleConn manages a single client connection's lifecycle.
func (p *Proxy) handleConn(clientConn net.Conn, connID int64) {
	defer p.activeConns.Done()

	clientAddr := clientConn.RemoteAddr().String()
	if p.verbose {
		log.Printf("[conn %d] opened from %s", connID, clientAddr)
	}

	prodConn, err := net.DialTimeout("tcp", p.prodAddr, 5*time.Second)
	if err != nil {
		log.Printf("[conn %d] failed to connect to prod %s: %v", connID, p.prodAddr, err)
		clientConn.Close()
		return
	}

	// WRITE GUARD: if routing is not available, refuse the connection to protect production.
	if !p.canRoute() {
		log.Printf("[conn %d] WRITE GUARD: shadow unavailable, refusing connection", connID)
		errResp := buildGuardErrorResponse("mori: shadow database unavailable, refusing connection to protect production")
		clientConn.Write(errResp)
		clientConn.Close()
		prodConn.Close()
		return
	}

	// Perform the startup handshake and connect to Shadow.
	prodConn, shadowConn, err := p.performStartup(clientConn, prodConn, connID)
	if err != nil || shadowConn == nil {
		log.Printf("[conn %d] WRITE GUARD: shadow handshake failed, refusing connection: %v", connID, err)
		errResp := buildGuardErrorResponse("mori: shadow database unavailable, refusing connection to protect production")
		clientConn.Write(errResp)
		clientConn.Close()
		prodConn.Close()
		return
	}

	guardedProd := NewSafeProdConn(prodConn, connID, p.verbose, p.logger)
	p.routeLoop(clientConn, guardedProd, shadowConn, connID)
}

// performStartup handles the PG startup handshake phase.
// It relays the client's startup sequence to Prod (transparent to client),
// and independently initiates a Shadow connection.
// Returns the (possibly TLS-upgraded) prodConn and the shadow connection.
func (p *Proxy) performStartup(clientConn, prodConn net.Conn, connID int64) (net.Conn, net.Conn, error) {
	prodConn, err := relayStartup(clientConn, prodConn, p.tlsParams)
	if err != nil {
		return prodConn, nil, fmt.Errorf("prod startup: %w", err)
	}

	shadowConn, err := p.connectShadow(connID)
	if err != nil {
		return prodConn, nil, fmt.Errorf("shadow startup: %w", err)
	}
	return prodConn, shadowConn, nil
}

// relayStartup relays the PG startup handshake between client and Prod.
// Handles SSLRequest, StartupMessage, auth exchanges, and waits for ReadyForQuery.
//
// SSL handling: the proxy listens on localhost only, so it declines SSL from
// the client ('N') and independently negotiates TLS with Prod when the server
// supports it. This avoids requiring the proxy to present its own TLS certificate.
func relayStartup(clientConn, prodConn net.Conn, tlsParams tlsutil.TLSParams) (net.Conn, error) {
	// Read initial message from client (no type byte).
	startupRaw, err := readStartupMsg(clientConn)
	if err != nil {
		return prodConn, fmt.Errorf("reading startup: %w", err)
	}

	// Handle SSLRequest: decline on the client side (localhost is safe),
	// then negotiate TLS with prod independently.
	if isSSLRequest(startupRaw) {
		// Tell client we don't support SSL on the local connection.
		if _, err := clientConn.Write([]byte{'N'}); err != nil {
			return prodConn, fmt.Errorf("declining SSL to client: %w", err)
		}
		// Client will now send the real StartupMessage in plaintext.
		startupRaw, err = readStartupMsg(clientConn)
		if err != nil {
			return prodConn, fmt.Errorf("reading startup after SSL decline: %w", err)
		}
	}

	// Negotiate TLS with Prod independently of the client.
	prodConn, err = negotiateProdSSL(prodConn, tlsParams)
	if err != nil {
		return prodConn, fmt.Errorf("prod SSL negotiation: %w", err)
	}

	// Forward StartupMessage to Prod (possibly over TLS).
	if _, err := prodConn.Write(startupRaw); err != nil {
		return prodConn, fmt.Errorf("forwarding startup: %w", err)
	}

	// Relay messages from Prod to client until ReadyForQuery ('Z').
	for {
		msg, err := readMsg(prodConn)
		if err != nil {
			return prodConn, fmt.Errorf("reading prod startup response: %w", err)
		}

		// If Prod sent an AuthenticationSASL (type 10), strip channel-binding
		// mechanisms (-PLUS) since the client is on plain TCP.
		if msg.Type == 'R' && len(msg.Payload) >= 4 {
			authType := binary.BigEndian.Uint32(msg.Payload[:4])
			if authType == 10 {
				msg = stripSASLPlusMechanisms(msg)
			}
		}

		if _, err := clientConn.Write(msg.Raw); err != nil {
			return prodConn, fmt.Errorf("relaying to client: %w", err)
		}

		if msg.Type == 'Z' {
			return prodConn, nil
		}

		// If Prod sent an auth request, the client may need to respond.
		// AuthenticationOk (0) and SASLFinal (12) are server-only messages
		// that do not require a client response.
		if msg.Type == 'R' && len(msg.Payload) >= 4 {
			authType := binary.BigEndian.Uint32(msg.Payload[:4])
			if authType != 0 && authType != 12 {
				clientMsg, err := readMsg(clientConn)
				if err != nil {
					return prodConn, fmt.Errorf("reading client auth response: %w", err)
				}
				if _, err := prodConn.Write(clientMsg.Raw); err != nil {
					return prodConn, fmt.Errorf("forwarding client auth: %w", err)
				}
			}
		}
	}
}

// negotiateProdSSL sends an SSLRequest to Prod and upgrades the connection
// to TLS based on the configured TLS parameters. Returns the original
// connection unchanged if SSLMode is "disable" or Prod declines SSL.
func negotiateProdSSL(prodConn net.Conn, params tlsutil.TLSParams) (net.Conn, error) {
	tlsCfg, err := tlsutil.BuildConfig(params)
	if err != nil {
		return prodConn, fmt.Errorf("building TLS config: %w", err)
	}
	if tlsCfg == nil {
		return prodConn, nil // SSLMode=disable
	}

	if _, err := prodConn.Write(buildSSLRequest()); err != nil {
		return prodConn, fmt.Errorf("sending SSLRequest to prod: %w", err)
	}

	var resp [1]byte
	if _, err := io.ReadFull(prodConn, resp[:]); err != nil {
		return prodConn, fmt.Errorf("reading prod SSL response: %w", err)
	}

	if resp[0] == 'S' {
		tlsConn := tls.Client(prodConn, tlsCfg)
		if err := tlsConn.Handshake(); err != nil {
			return prodConn, fmt.Errorf("TLS handshake with prod: %w", err)
		}
		return tlsConn, nil
	}

	// Prod declined SSL — error if mode requires it.
	if params.SSLMode == "require" || params.SSLMode == "verify-ca" || params.SSLMode == "verify-full" {
		return prodConn, fmt.Errorf("server does not support SSL but sslmode=%s requires it", params.SSLMode)
	}

	return prodConn, nil
}

// connectShadow establishes a TCP connection to the Shadow database and
// performs a PG startup handshake using Shadow credentials.
func (p *Proxy) connectShadow(connID int64) (net.Conn, error) {
	shadowConn, err := net.DialTimeout("tcp", p.shadowAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial shadow %s: %w", p.shadowAddr, err)
	}

	startupMsg := buildStartupMessage("postgres", p.shadowDBName)
	if _, err := shadowConn.Write(startupMsg); err != nil {
		shadowConn.Close()
		return nil, fmt.Errorf("sending startup to shadow: %w", err)
	}

	for {
		msg, err := readMsg(shadowConn)
		if err != nil {
			shadowConn.Close()
			return nil, fmt.Errorf("reading shadow response: %w", err)
		}

		switch msg.Type {
		case 'Z': // ReadyForQuery — Shadow is ready.
			if p.verbose {
				log.Printf("[conn %d] Shadow connection established", connID)
			}
			return shadowConn, nil

		case 'R': // Authentication request.
			if len(msg.Payload) >= 4 {
				authType := binary.BigEndian.Uint32(msg.Payload[:4])
				switch authType {
				case 0: // AuthenticationOk — continue reading.
				case 3: // CleartextPassword
					if _, err := shadowConn.Write(buildPasswordMessage("mori")); err != nil {
						shadowConn.Close()
						return nil, fmt.Errorf("sending shadow password: %w", err)
					}
				case 5: // MD5Password
					if len(msg.Payload) < 8 {
						shadowConn.Close()
						return nil, fmt.Errorf("MD5 auth: payload too short")
					}
					md5Msg := buildMD5PasswordMessage("postgres", "mori", msg.Payload[4:8])
					if _, err := shadowConn.Write(md5Msg); err != nil {
						shadowConn.Close()
						return nil, fmt.Errorf("sending shadow MD5 password: %w", err)
					}
				case 10: // SASL — not supported for Shadow.
					shadowConn.Close()
					return nil, fmt.Errorf("shadow requires SASL auth (not supported)")
				}
			}

		case 'E': // Error from Shadow.
			shadowConn.Close()
			return nil, fmt.Errorf("shadow startup error: %s", string(msg.Payload))

		default:
			// ParameterStatus ('S'), BackendKeyData ('K') — ignore.
		}
	}
}

type routeTarget int

const (
	targetProd   routeTarget = iota
	targetShadow
	targetBoth
)

// routeDecision captures the routing target along with classification details
// needed by write path handlers.
type routeDecision struct {
	target         routeTarget
	classification *core.Classification // nil for non-Query messages
	strategy       core.RoutingStrategy
}

// routeLoop is the main query routing loop for a connection.
func (p *Proxy) routeLoop(clientConn, prodConn, shadowConn net.Conn, connID int64) {
	var closeOnce sync.Once
	closeAll := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			prodConn.Close()
			shadowConn.Close()
		})
	}
	defer closeAll()

	// Create a WriteHandler for this connection if write-path state is available.
	var wh *WriteHandler
	if p.deltaMap != nil && p.tombstones != nil {
		wh = &WriteHandler{
			prodConn:       prodConn,
			shadowConn:     shadowConn,
			deltaMap:       p.deltaMap,
			tombstones:     p.tombstones,
			tables:         p.tables,
			schemaRegistry: p.schemaRegistry,
			moriDir:        p.moriDir,
			connID:         connID,
			verbose:        p.verbose,
			logger:         p.logger,
			maxRowsHydrate: p.maxRowsHydrate,
		}
	}

	// Create a ReadHandler for merged reads.
	var rh *ReadHandler
	if p.deltaMap != nil && p.tombstones != nil {
		rh = &ReadHandler{
			prodConn:       prodConn,
			shadowConn:     shadowConn,
			deltaMap:       p.deltaMap,
			tombstones:     p.tombstones,
			tables:         p.tables,
			schemaRegistry: p.schemaRegistry,
			connID:         connID,
			verbose:        p.verbose,
			logger:         p.logger,
			maxRowsHydrate: p.maxRowsHydrate,
			isCockroachDB:  p.isCockroachDB,
		}
	}

	// Create a DDLHandler for schema-tracking DDL execution.
	var ddh *DDLHandler
	if p.schemaRegistry != nil {
		ddh = &DDLHandler{
			shadowConn:     shadowConn,
			schemaRegistry: p.schemaRegistry,
			deltaMap:       p.deltaMap,
			tombstones:     p.tombstones,
			moriDir:        p.moriDir,
			connID:         connID,
			verbose:        p.verbose,
			logger:         p.logger,
		}
	}

	// Create a TxnHandler for coordinated transaction control.
	var txh *TxnHandler
	if p.deltaMap != nil && p.tombstones != nil {
		txh = &TxnHandler{
			prodConn:       prodConn,
			shadowConn:     shadowConn,
			deltaMap:       p.deltaMap,
			tombstones:     p.tombstones,
			schemaRegistry: p.schemaRegistry,
			moriDir:        p.moriDir,
			connID:         connID,
			verbose:        p.verbose,
			logger:         p.logger,
			isCockroachDB:  p.isCockroachDB,
		}
	}

	// Link TxnHandler to WriteHandler for staged delta awareness.
	if wh != nil && txh != nil {
		wh.txnHandler = txh
	}

	// Create FKEnforcer and link to WriteHandler for FK constraint enforcement.
	if wh != nil && p.schemaRegistry != nil {
		wh.fkEnforcer = &FKEnforcer{
			prodConn:       prodConn,
			shadowConn:     shadowConn,
			deltaMap:       p.deltaMap,
			tombstones:     p.tombstones,
			tables:         p.tables,
			schemaRegistry: p.schemaRegistry,
			connID:         connID,
			verbose:        p.verbose,
			logger:         p.logger,
		}
	}

	// Create an ExtHandler for extended query protocol (Parse/Bind/Execute/...).
	var eh *ExtHandler
	if p.classifier != nil && p.router != nil {
		eh = &ExtHandler{
			prodConn:       prodConn,
			shadowConn:     shadowConn,
			classifier:     p.classifier,
			router:         p.router,
			deltaMap:       p.deltaMap,
			tombstones:     p.tombstones,
			tables:         p.tables,
			schemaRegistry: p.schemaRegistry,
			moriDir:        p.moriDir,
			connID:         connID,
			verbose:        p.verbose,
			logger:         p.logger,
			txnHandler:     txh,
			writeHandler:   wh,
			readHandler:    rh,
			stmtCache:       make(map[string]string),
			shadowOnlyStmts: make(map[string]bool),
		}
	}

	// SQL-level PREPARE cache: maps statement name → original SQL.
	prepareCache := make(map[string]string)

	for {
		msg, err := readMsg(clientConn)
		if err != nil {
			if err != io.EOF && p.verbose {
				log.Printf("[conn %d] client read error: %v", connID, err)
			}
			return
		}

		// Terminate: forward to both backends.
		if msg.Type == 'X' {
			prodConn.Write(msg.Raw)
			shadowConn.Write(msg.Raw)
			if p.verbose {
				log.Printf("[conn %d] terminated", connID)
			}
			return
		}

		// Extended query protocol: accumulate messages, dispatch on Sync.
		if isExtendedProtocolMsg(msg.Type) && eh != nil {
			eh.Accumulate(msg)
			if msg.Type == 'S' { // Sync triggers batch processing.
				if err := eh.FlushBatch(clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] extended query error: %v", connID, err)
					}
					return
				}
			}
			continue
		}

		decision := p.classifyAndRoute(msg, connID)

		// SQL-level PREPARE: cache the SQL and forward to Shadow.
		if decision.classification != nil && decision.classification.SubType == core.SubPrepare {
			name, innerSQL := parsePrepareStmt(decision.classification.RawSQL)
			if name != "" && innerSQL != "" {
				prepareCache[name] = innerSQL
				if p.verbose {
					log.Printf("[conn %d] PREPARE %s: cached SQL (%d chars)", connID, name, len(innerSQL))
				}
				p.logger.Event(connID, "prepare", fmt.Sprintf("PREPARE %s", name))
			}
			// Forward to Shadow and relay response.
			if err := forwardAndRelay(msg.Raw, shadowConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] shadow relay error (prepare): %v", connID, err)
				}
				return
			}
			continue
		}

		// SQL-level EXECUTE: look up cached SQL, substitute params, reclassify, re-route.
		if decision.classification != nil && decision.classification.SubType == core.SubExecute {
			name, params := parseExecuteStmt(decision.classification.RawSQL)
			cachedSQL, ok := prepareCache[name]
			if !ok {
				// Not in our cache — might have been prepared before proxy started.
				// Fall through to Prod (best effort).
				if p.verbose {
					log.Printf("[conn %d] EXECUTE %s: not in cache, forwarding to prod", connID, name)
				}
				if err := forwardAndRelay(msg.Raw, prodConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] prod relay error (execute): %v", connID, err)
					}
					return
				}
				continue
			}

			// Substitute parameters into the SQL.
			fullSQL := substituteExecuteParams(cachedSQL, params)
			if p.verbose {
				log.Printf("[conn %d] EXECUTE %s: resolved to: %s", connID, name, truncateSQL(fullSQL, 100))
			}

			// Reclassify with the actual SQL.
			cl, err := p.classifier.Classify(fullSQL)
			if err != nil {
				if p.verbose {
					log.Printf("[conn %d] EXECUTE %s: reclassify error, forwarding to shadow: %v", connID, name, err)
				}
				if err := forwardAndRelay(msg.Raw, shadowConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] shadow relay error (execute reclassify): %v", connID, err)
					}
					return
				}
				continue
			}

			// Re-route based on the actual query.
			strategy := p.router.Route(cl)

			// Build a query message with the full SQL for execution.
			fullMsg := buildQueryMsg(fullSQL)

			p.logger.Query(connID, fullSQL, cl, strategy, 0)

			// Dispatch based on strategy — same as normal routing.
			// For reads, use the read handler. For writes, use the write handler.
			handled := false
			if wh != nil {
				switch strategy {
				case core.StrategyShadowWrite, core.StrategyHydrateAndWrite, core.StrategyShadowDelete:
					if err := wh.HandleWrite(clientConn, fullMsg, cl, strategy); err != nil {
						if p.verbose {
							log.Printf("[conn %d] write handler error (execute): %v", connID, err)
						}
						return
					}
					handled = true
				}
			}
			if !handled && rh != nil {
				switch strategy {
				case core.StrategyMergedRead, core.StrategyJoinPatch:
					if err := rh.HandleRead(clientConn, fullMsg, cl, strategy); err != nil {
						if p.verbose {
							log.Printf("[conn %d] read handler error (execute): %v", connID, err)
						}
						return
					}
					handled = true
				}
			}
			if !handled {
				// Default: forward the original EXECUTE to Shadow (where the PREPARE exists).
				if err := forwardAndRelay(msg.Raw, shadowConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] shadow relay error (execute default): %v", connID, err)
					}
					return
				}
			}
			continue
		}

		// SQL-level DEALLOCATE: remove from cache and forward to shadow only
		// (PREPARE was only sent to shadow, so Prod doesn't know the statement).
		if decision.classification != nil && decision.classification.SubType == core.SubDeallocate {
			name := parseDeallocateStmt(decision.classification.RawSQL)
			if name != "" {
				delete(prepareCache, name)
				if p.verbose {
					log.Printf("[conn %d] DEALLOCATE %s: removed from cache", connID, name)
				}
			}
			if err := forwardAndRelay(msg.Raw, shadowConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] DEALLOCATE forward error: %v", connID, err)
				}
				return
			}
			continue
		}

		// Dispatch write strategies to WriteHandler when available.
		if wh != nil && decision.classification != nil {
			switch decision.strategy {
			case core.StrategyShadowWrite,
				core.StrategyHydrateAndWrite,
				core.StrategyShadowDelete:
				if err := wh.HandleWrite(clientConn, msg.Raw, decision.classification, decision.strategy); err != nil {
					if p.verbose {
						log.Printf("[conn %d] write handler error: %v", connID, err)
					}
					return
				}
				continue
			}
		}

		// Dispatch merged read strategies to ReadHandler when available.
		if rh != nil && decision.classification != nil {
			switch decision.strategy {
			case core.StrategyMergedRead,
				core.StrategyJoinPatch:
				if err := rh.HandleRead(clientConn, msg.Raw, decision.classification, decision.strategy); err != nil {
					if p.verbose {
						log.Printf("[conn %d] read handler error: %v", connID, err)
					}
					return
				}
				continue
			}
		}

		// Dispatch DDL to DDLHandler when available.
		if ddh != nil && decision.classification != nil && decision.strategy == core.StrategyShadowDDL {
			if err := ddh.HandleDDL(clientConn, msg.Raw, decision.classification); err != nil {
				if p.verbose {
					log.Printf("[conn %d] DDL handler error: %v", connID, err)
				}
				return
			}
			continue
		}

		// Dispatch transaction control to TxnHandler when available.
		if txh != nil && decision.classification != nil && decision.strategy == core.StrategyTransaction {
			if err := txh.HandleTxn(clientConn, msg.Raw, decision.classification); err != nil {
				if p.verbose {
					log.Printf("[conn %d] txn handler error: %v", connID, err)
				}
				return
			}
			continue
		}

		// Handle not-supported features.
		if decision.classification != nil && decision.strategy == core.StrategyNotSupported {
			errMsg := core.UnsupportedTransactionMsg
			if decision.classification.NotSupportedMsg != "" {
				errMsg = decision.classification.NotSupportedMsg
			} else if decision.classification.SubType == core.SubExplain {
				errMsg = "EXPLAIN is not supported on tables with pending changes"
			}
			errResp := buildGuardErrorResponse(errMsg)
			clientConn.Write(errResp)
			continue
		}

		// Handle SET/DEALLOCATE — forward to both backends.
		if decision.classification != nil && decision.strategy == core.StrategyForwardBoth {
			// Send to Shadow first, drain response.
			shadowConn.Write(msg.Raw)
			if err := drainUntilReady(shadowConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] shadow drain error (forward both): %v", connID, err)
				}
			}
			// Forward to Prod and relay response to client.
			if err := forwardAndRelay(msg.Raw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] prod relay error (forward both): %v", connID, err)
				}
				return
			}
			continue
		}

		// Handle LISTEN/UNLISTEN — forward to Prod only.
		if decision.classification != nil && decision.strategy == core.StrategyListenOnly {
			if err := forwardAndRelay(msg.Raw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] prod relay error (listen): %v", connID, err)
				}
				return
			}
			continue
		}

		// Handle TRUNCATE — forward to Shadow, mark fully shadowed.
		if decision.classification != nil && decision.strategy == core.StrategyTruncate {
			if err := forwardAndRelay(msg.Raw, shadowConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] shadow relay error (truncate): %v", connID, err)
				}
				return
			}
			// Mark all truncated tables as fully shadowed.
			if p.schemaRegistry != nil {
				// Detect CASCADE behavior from the TRUNCATE AST.
				isCascade := false
				if result, err := pg_query.Parse(decision.classification.RawSQL); err == nil {
					for _, stmt := range result.GetStmts() {
						if ts := stmt.GetStmt().GetTruncateStmt(); ts != nil {
							if ts.GetBehavior() == pg_query.DropBehavior_DROP_CASCADE {
								isCascade = true
							}
							break
						}
					}
				}

				if isCascade {
					// CASCADE: mark explicit tables and all FK-referenced children.
					markCascadedTablesFullyShadowed(decision.classification.Tables, p.schemaRegistry, p.tombstones)
					if p.verbose {
						log.Printf("[conn %d] TRUNCATE CASCADE: marked tables and FK children as fully shadowed", connID)
					}
				} else {
					// Non-CASCADE: mark only the explicit tables.
					for _, table := range decision.classification.Tables {
						p.schemaRegistry.MarkFullyShadowed(table)
						if p.verbose {
							log.Printf("[conn %d] TRUNCATE: marked %s as fully shadowed", connID, table)
						}
					}
					if p.tombstones != nil {
						for _, table := range decision.classification.Tables {
							p.tombstones.ClearTable(table)
						}
					}
				}
				// Persist registry.
				coreSchema.WriteRegistry(p.moriDir, p.schemaRegistry)
			}
			p.logger.Event(connID, "truncate", fmt.Sprintf("tables=%v", decision.classification.Tables))
			continue
		}

		switch decision.target {
		case targetProd:
			// WRITE GUARD L3: final check before prod dispatch.
			if decision.classification != nil &&
				(decision.classification.OpType == core.OpWrite || decision.classification.OpType == core.OpDDL) {
				log.Printf("[CRITICAL] [conn %d] WRITE GUARD L3: %s/%s reached targetProd — BLOCKED",
					connID, decision.classification.OpType, decision.classification.SubType)
				errResp := buildGuardErrorResponse("mori: write operation blocked — internal routing error detected")
				clientConn.Write(errResp)
				continue
			}
			if err := forwardAndRelay(msg.Raw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] prod relay error: %v", connID, err)
				}
				return
			}

		case targetShadow:
			if err := forwardAndRelay(msg.Raw, shadowConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] shadow relay error: %v", connID, err)
				}
				return
			}

		case targetBoth:
			// Send to Shadow first, drain its response.
			shadowConn.Write(msg.Raw)
			if err := drainUntilReady(shadowConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] shadow drain error: %v", connID, err)
				}
			}
			// Then forward to Prod and relay response to client.
			if err := forwardAndRelay(msg.Raw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] prod relay error (both): %v", connID, err)
				}
				return
			}
		}
	}
}

// classifyAndRoute determines which backend should handle this message.
func (p *Proxy) classifyAndRoute(msg *pgMsg, connID int64) routeDecision {
	// Only classify Query ('Q') messages.
	if msg.Type != 'Q' {
		return routeDecision{target: targetProd}
	}

	sql := querySQL(msg.Payload)
	if sql == "" {
		return routeDecision{target: targetProd}
	}

	start := time.Now()

	classification, err := p.classifier.Classify(sql)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] classify error, forwarding to prod: %v", connID, err)
		}
		return routeDecision{target: targetProd}
	}

	strategy := p.router.Route(classification)

	// WRITE GUARD L1: validate routing decision.
	if err := validateRouteDecision(classification, strategy, connID, p.logger); err != nil {
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       core.StrategyShadowWrite,
		}
	}

	elapsed := time.Since(start)

	if p.verbose {
		log.Printf("[conn %d] %s/%s tables=%v → %s | %s",
			connID, classification.OpType, classification.SubType,
			classification.Tables, strategy, truncateSQL(sql, 100))
	}

	p.logger.Query(connID, sql, classification, strategy, elapsed)

	switch strategy {
	case core.StrategyShadowWrite,
		core.StrategyHydrateAndWrite,
		core.StrategyShadowDelete:
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyShadowDDL:
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyTransaction:
		return routeDecision{target: targetBoth, classification: classification, strategy: strategy}

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		return routeDecision{
			target:         targetProd, // fallback if ReadHandler is nil
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyNotSupported:
		return routeDecision{target: targetProd, classification: classification, strategy: strategy}

	case core.StrategyForwardBoth:
		return routeDecision{target: targetBoth, classification: classification, strategy: strategy}

	case core.StrategyListenOnly:
		return routeDecision{target: targetProd, classification: classification, strategy: strategy}

	case core.StrategyTruncate:
		return routeDecision{target: targetShadow, classification: classification, strategy: strategy}

	case core.StrategyProdDirect:
		// Override metadata queries about dirty tables
		// so that \d and information_schema queries reflect Shadow schema.
		if classification != nil && classification.IsMetadataQuery &&
			classification.IntrospectedTable != "" && p.schemaRegistry != nil &&
			p.schemaRegistry.HasDiff(classification.IntrospectedTable) {
			return routeDecision{target: targetShadow, classification: classification, strategy: strategy}
		}
		return routeDecision{target: targetProd, classification: classification, strategy: strategy}

	default:
		return routeDecision{target: targetProd, classification: classification, strategy: core.StrategyNotSupported}
	}
}

// forwardAndRelay sends a message to the backend, then reads and relays
// the complete response back to the client until ReadyForQuery ('Z').
func forwardAndRelay(raw []byte, backend, client net.Conn) error {
	if _, err := backend.Write(raw); err != nil {
		return fmt.Errorf("sending to backend: %w", err)
	}

	for {
		msg, err := readMsg(backend)
		if err != nil {
			return fmt.Errorf("reading backend response: %w", err)
		}

		if _, err := client.Write(msg.Raw); err != nil {
			return fmt.Errorf("relaying to client: %w", err)
		}

		if msg.Type == 'Z' {
			return nil
		}
	}
}

// forwardRelayAndCaptureTag sends a message to the backend, relays the full
// response to the client, and returns the CommandComplete tag string.
func forwardRelayAndCaptureTag(raw []byte, backend, client net.Conn) (string, error) {
	if _, err := backend.Write(raw); err != nil {
		return "", fmt.Errorf("sending to backend: %w", err)
	}

	var tag string
	for {
		msg, err := readMsg(backend)
		if err != nil {
			return tag, fmt.Errorf("reading backend response: %w", err)
		}

		if msg.Type == 'C' {
			tag = parseCommandTag(msg.Payload)
		}

		if _, err := client.Write(msg.Raw); err != nil {
			return tag, fmt.Errorf("relaying to client: %w", err)
		}

		if msg.Type == 'Z' {
			return tag, nil
		}
	}
}

// forwardAndCapture sends a message to the backend and collects the complete
// response (until ReadyForQuery). Returns all response messages without relaying.
func forwardAndCapture(raw []byte, backend net.Conn) ([]*pgMsg, error) {
	if _, err := backend.Write(raw); err != nil {
		return nil, fmt.Errorf("sending to backend: %w", err)
	}

	var msgs []*pgMsg
	for {
		msg, err := readMsg(backend)
		if err != nil {
			return msgs, fmt.Errorf("reading backend response: %w", err)
		}
		msgs = append(msgs, msg)
		if msg.Type == 'Z' {
			return msgs, nil
		}
	}
}

// drainUntilReady reads and discards messages from a connection until ReadyForQuery.
func drainUntilReady(conn net.Conn) error {
	for {
		msg, err := readMsg(conn)
		if err != nil {
			return err
		}
		if msg.Type == 'Z' {
			return nil
		}
	}
}

func truncateSQL(sql string, maxLen int) string {
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}

// parsePrepareStmt extracts the name and SQL from "PREPARE name [(types)] AS sql".
func parsePrepareStmt(rawSQL string) (name, innerSQL string) {
	// Use pg_query to parse robustly.
	result, err := pg_query.Parse(rawSQL)
	if err != nil {
		return "", ""
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return "", ""
	}
	prep := stmts[0].GetStmt().GetPrepareStmt()
	if prep == nil {
		return "", ""
	}
	name = prep.GetName()
	// Deparse the inner query.
	innerResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{Stmt: prep.GetQuery()},
		},
	}
	innerSQL, err = pg_query.Deparse(innerResult)
	if err != nil {
		return name, ""
	}
	return name, innerSQL
}

// parseExecuteStmt extracts the name and parameter values from "EXECUTE name(param1, param2, ...)".
// Parameters are returned as string literals.
func parseExecuteStmt(rawSQL string) (name string, params []string) {
	result, err := pg_query.Parse(rawSQL)
	if err != nil {
		return "", nil
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return "", nil
	}
	exec := stmts[0].GetStmt().GetExecuteStmt()
	if exec == nil {
		return "", nil
	}
	name = exec.GetName()
	for _, p := range exec.GetParams() {
		if ac := p.GetAConst(); ac != nil {
			if sv := ac.GetSval(); sv != nil {
				params = append(params, "'"+sv.GetSval()+"'")
			} else if iv := ac.GetIval(); iv != nil {
				params = append(params, fmt.Sprintf("%d", iv.GetIval()))
			} else if fv := ac.GetFval(); fv != nil {
				params = append(params, fv.GetFval())
			} else if ac.GetIsnull() {
				params = append(params, "NULL")
			} else if bv := ac.GetBoolval(); bv != nil {
				if bv.GetBoolval() {
					params = append(params, "TRUE")
				} else {
					params = append(params, "FALSE")
				}
			}
		} else if tc := p.GetTypeCast(); tc != nil {
			// Type cast: recurse on the argument and append ::type
			if arg := tc.GetArg(); arg != nil {
				if ac := arg.GetAConst(); ac != nil {
					var val string
					if sv := ac.GetSval(); sv != nil {
						val = "'" + sv.GetSval() + "'"
					} else if iv := ac.GetIval(); iv != nil {
						val = fmt.Sprintf("%d", iv.GetIval())
					}
					// For simplicity, just use the value without the cast.
					params = append(params, val)
				}
			}
		}
	}
	return name, params
}

// substituteExecuteParams replaces $1, $2, ... placeholders in SQL with parameter values.
func substituteExecuteParams(sql string, params []string) string {
	result := sql
	// Replace in reverse order to handle $10, $11, etc. correctly.
	for i := len(params); i >= 1; i-- {
		placeholder := fmt.Sprintf("$%d", i)
		if i-1 < len(params) {
			result = strings.ReplaceAll(result, placeholder, params[i-1])
		}
	}
	return result
}

// parseDeallocateStmt extracts the statement name from "DEALLOCATE [PREPARE] name".
func parseDeallocateStmt(rawSQL string) string {
	result, err := pg_query.Parse(rawSQL)
	if err != nil {
		return ""
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return ""
	}
	dealloc := stmts[0].GetStmt().GetDeallocateStmt()
	if dealloc == nil {
		return ""
	}
	return dealloc.GetName()
}

// markCascadedTablesFullyShadowed marks the given tables and all their FK children
// (recursively) as fully shadowed, and clears their tombstones. This is needed for
// TRUNCATE CASCADE, where PostgreSQL automatically truncates child tables that
// have foreign key references to the truncated table.
func markCascadedTablesFullyShadowed(tables []string, schemaRegistry *coreSchema.Registry, tombstones *delta.TombstoneSet) {
	visited := make(map[string]bool)
	var walk func(table string)
	walk = func(table string) {
		if visited[table] {
			return
		}
		visited[table] = true
		schemaRegistry.MarkFullyShadowed(table)
		if tombstones != nil {
			tombstones.ClearTable(table)
		}
		for _, fk := range schemaRegistry.GetReferencingFKs(table) {
			walk(fk.ChildTable)
		}
	}
	for _, t := range tables {
		walk(t)
	}
}
