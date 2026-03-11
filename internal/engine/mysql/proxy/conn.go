package proxy

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/psrth/mori/internal/core"
)

type routeTarget int

const (
	targetProd   routeTarget = iota
	targetShadow
	targetBoth
)

type routeDecision struct {
	target         routeTarget
	classification *core.Classification
	strategy       core.RoutingStrategy
}

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

	// WRITE GUARD: if routing is not available, refuse the connection.
	if !p.canRoute() {
		log.Printf("[conn %d] WRITE GUARD: shadow unavailable, refusing connection", connID)
		errPkt := buildGuardErrorResponse(0, "shadow database unavailable, refusing connection to protect production")
		clientConn.Write(errPkt)
		clientConn.Close()
		prodConn.Close()
		return
	}

	// Relay handshake between client and prod (may upgrade prodConn to TLS).
	prodConn, err = relayHandshake(clientConn, prodConn, p.tlsParams)
	if err != nil {
		log.Printf("[conn %d] handshake failed: %v", connID, err)
		clientConn.Close()
		prodConn.Close()
		return
	}

	// Connect to shadow.
	shadowConn, err := connectShadow(p.shadowAddr, p.shadowDBName)
	if err != nil {
		log.Printf("[conn %d] WRITE GUARD: shadow connection failed, refusing: %v", connID, err)
		errPkt := buildGuardErrorResponse(1, "shadow database unavailable, refusing connection to protect production")
		clientConn.Write(errPkt)
		clientConn.Close()
		prodConn.Close()
		return
	}

	guardedProd := NewSafeProdConn(prodConn, connID, p.verbose, p.logger)
	p.routeLoop(clientConn, guardedProd, shadowConn, connID)
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

	// Create an FKEnforcer for foreign key validation.
	var fke *FKEnforcer
	if p.deltaMap != nil && p.tombstones != nil && p.schemaRegistry != nil {
		fke = &FKEnforcer{
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

	// Create a WriteHandler for delta-tracking writes.
	var wh *WriteHandler
	if p.deltaMap != nil && p.tombstones != nil {
		wh = &WriteHandler{
			prodConn:       prodConn,
			shadowConn:     shadowConn,
			deltaMap:       p.deltaMap,
			tombstones:     p.tombstones,
			tables:         p.tables,
			schemaRegistry: p.schemaRegistry,
			fkEnforcer:     fke,
			moriDir:        p.moriDir,
			connID:         connID,
			verbose:        p.verbose,
			logger:         p.logger,
			maxRowsHydrate: p.maxRowsHydrate,
			useReturning:   p.useReturning,
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

	// Create a TxnHandler for transaction coordination.
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
		}
	}

	// Create an ExtHandler for prepared statement support.
	var exh *ExtHandler
	if p.deltaMap != nil && p.tombstones != nil {
		exh = &ExtHandler{
			prodConn:     prodConn,
			shadowConn:   shadowConn,
			classifier:   p.classifier,
			router:       p.router,
			deltaMap:     p.deltaMap,
			tombstones:   p.tombstones,
			tables:       p.tables,
			moriDir:      p.moriDir,
			connID:       connID,
			verbose:      p.verbose,
			logger:       p.logger,
			txnHandler:   txh,
			writeHandler: wh,
			readHandler:  rh,
			stmtCache:    make(map[string]stmtCacheEntry),
		}
	}

	// Wire ExtHandler into the L2 write guard so it can inspect COM_STMT_EXECUTE.
	if exh != nil {
		if spc, ok := prodConn.(*SafeProdConn); ok {
			spc.SetExtHandler(exh)
		}
	}

	for {
		pkt, err := readMySQLPacket(clientConn)
		if err != nil {
			if err != io.EOF && p.verbose {
				log.Printf("[conn %d] client read error: %v", connID, err)
			}
			return
		}

		if len(pkt.Payload) == 0 {
			continue
		}

		cmd := pkt.Payload[0]

		// COM_QUIT: forward to both backends.
		if cmd == comQuit {
			prodConn.Write(pkt.Raw)
			shadowConn.Write(pkt.Raw)
			if p.verbose {
				log.Printf("[conn %d] terminated (COM_QUIT)", connID)
			}
			return
		}

		// COM_PING: respond with OK.
		if cmd == comPing {
			clientConn.Write(buildOKPacket(1))
			continue
		}

		// COM_INIT_DB: forward to both backends, relay prod response.
		if cmd == comInitDB {
			shadowConn.Write(pkt.Raw)
			drainResponse(shadowConn)
			if err := forwardAndRelay(pkt.Raw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] COM_INIT_DB relay error: %v", connID, err)
				}
				return
			}
			continue
		}

		// COM_FIELD_LIST: forward to prod, relay response.
		if cmd == comFieldList {
			if err := forwardAndRelay(pkt.Raw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] COM_FIELD_LIST relay error: %v", connID, err)
				}
				return
			}
			continue
		}

		// COM_STMT_RESET: forward to both backends (reset state on both).
		if cmd == comStmtReset {
			shadowConn.Write(pkt.Raw)
			drainResponse(shadowConn)
			if err := forwardAndRelay(pkt.Raw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] COM_STMT_RESET relay error: %v", connID, err)
				}
				return
			}
			continue
		}

		// COM_STMT_SEND_LONG_DATA: forward to both backends (buffer chunks before EXECUTE).
		// No response is sent for this command.
		if cmd == comStmtSendLongData {
			shadowConn.Write(pkt.Raw)
			prodConn.Write(pkt.Raw)
			continue
		}

		// COM_STMT_FETCH: forward to shadow (where prepared statements live).
		if cmd == comStmtFetch {
			if err := forwardAndRelay(pkt.Raw, shadowConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] COM_STMT_FETCH relay error: %v", connID, err)
				}
				return
			}
			continue
		}

		// COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_CLOSE:
		// Route through ExtHandler for classified routing of prepared statements.
		if cmd == comStmtPrepare || cmd == comStmtExecute || cmd == comStmtClose {
			if exh != nil {
				if err := exh.HandleCommand(clientConn, pkt, cmd); err != nil {
					if p.verbose {
						log.Printf("[conn %d] ext handler error: %v", connID, err)
					}
					return
				}
				continue
			}
			// Fallback: forward to prod.
			if err := forwardAndRelay(pkt.Raw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] prepared stmt relay error: %v", connID, err)
				}
				return
			}
			continue
		}

		// COM_QUERY: classify and route.
		if cmd == comQuery {
			sql := extractQuerySQL(pkt.Payload)
			decision := p.classifyAndRoute(sql, connID)

			// Dispatch transaction control to TxnHandler when available.
			if txh != nil && decision.classification != nil && decision.strategy == core.StrategyTransaction {
				if err := txh.HandleTxn(clientConn, pkt.Raw, decision.classification); err != nil {
					if p.verbose {
						log.Printf("[conn %d] txn handler error: %v", connID, err)
					}
					return
				}
				continue
			}

			// Dispatch write strategies to WriteHandler when available.
			// Inside a transaction, use Stage instead of Add for delta tracking.
			if wh != nil && decision.classification != nil {
				switch decision.strategy {
				case core.StrategyShadowWrite,
					core.StrategyHydrateAndWrite,
					core.StrategyShadowDelete:
					if txh != nil && txh.InTxn() {
						if err := handleTxnWrite(wh, txh, clientConn, pkt.Raw, decision.classification, decision.strategy); err != nil {
							if p.verbose {
								log.Printf("[conn %d] txn write handler error: %v", connID, err)
							}
							return
						}
					} else {
						if err := wh.HandleWrite(clientConn, pkt.Raw, decision.classification, decision.strategy); err != nil {
							if p.verbose {
								log.Printf("[conn %d] write handler error: %v", connID, err)
							}
							return
						}
					}
					continue
				}
			}

			// Dispatch merged read strategies to ReadHandler when available.
			if rh != nil && decision.classification != nil {
				switch decision.strategy {
				case core.StrategyMergedRead, core.StrategyJoinPatch:
					// Shadow-only shortcut: if any table is fully shadowed, execute on Shadow only.
					// This prevents the merged read path from trying to query Prod for tables
					// that don't exist there (e.g., tables created via DDL on Shadow).
					if p.schemaRegistry != nil && decision.classification != nil {
						allShadow := false
						for _, t := range decision.classification.Tables {
							if p.schemaRegistry.IsFullyShadowed(t) {
								allShadow = true
								break
							}
						}
						if allShadow {
							if err := forwardAndRelay(pkt.Raw, shadowConn, clientConn); err != nil {
								if p.verbose {
									log.Printf("[conn %d] shadow-only read relay error: %v", connID, err)
								}
								return
							}
							continue
						}
					}
					if err := rh.HandleRead(clientConn, pkt.Raw, decision.classification, decision.strategy); err != nil {
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
				if err := ddh.HandleDDL(clientConn, pkt.Raw, decision.classification); err != nil {
					if p.verbose {
						log.Printf("[conn %d] DDL handler error: %v", connID, err)
					}
					return
				}
				continue
			}

			// Handle TRUNCATE: forward to Shadow, mark fully shadowed.
			if decision.strategy == core.StrategyTruncate && p.schemaRegistry != nil {
				if err := forwardAndRelay(pkt.Raw, shadowConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] truncate relay error: %v", connID, err)
					}
					return
				}
				for _, table := range decision.classification.Tables {
					p.schemaRegistry.MarkFullyShadowed(table)
					if p.deltaMap != nil {
						p.deltaMap.ClearTable(table)
					}
					if p.tombstones != nil {
						p.tombstones.ClearTable(table)
					}
				}
				if p.verbose {
					log.Printf("[conn %d] TRUNCATE: tables %v marked fully shadowed", connID, decision.classification.Tables)
				}
				p.logger.Event(connID, "truncate", fmt.Sprintf("tables=%v", decision.classification.Tables))
				continue
			}

			// Handle NotSupported: return error to client.
			if decision.strategy == core.StrategyNotSupported && decision.classification != nil {
				msg := decision.classification.NotSupportedMsg
				if msg == "" {
					msg = core.UnsupportedTransactionMsg
				}
				errPkt := buildGuardErrorResponse(1, msg)
				clientConn.Write(errPkt)
				continue
			}

			switch decision.target {
			case targetProd:
				// WRITE GUARD L3: final check before prod dispatch.
				if decision.classification != nil &&
					(decision.classification.OpType == core.OpWrite || decision.classification.OpType == core.OpDDL) {
					log.Printf("[CRITICAL] [conn %d] WRITE GUARD L3: %s/%s reached targetProd — BLOCKED",
						connID, decision.classification.OpType, decision.classification.SubType)
					errPkt := buildGuardErrorResponse(1, "write operation blocked — internal routing error detected")
					clientConn.Write(errPkt)
					continue
				}
				if err := forwardAndRelay(pkt.Raw, prodConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] prod relay error: %v", connID, err)
					}
					return
				}

			case targetShadow:
				if err := forwardAndRelay(pkt.Raw, shadowConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] shadow relay error: %v", connID, err)
					}
					return
				}

			case targetBoth:
				// Send to shadow first, drain response.
				shadowConn.Write(pkt.Raw)
				if err := drainResponse(shadowConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] shadow drain error: %v", connID, err)
					}
				}
				// Forward to prod and relay response.
				if err := forwardAndRelay(pkt.Raw, prodConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] prod relay error (both): %v", connID, err)
					}
					return
				}
			}
			continue
		}

		// Unknown command: forward to prod.
		if err := forwardAndRelay(pkt.Raw, prodConn, clientConn); err != nil {
			if p.verbose {
				log.Printf("[conn %d] unknown cmd relay error: %v", connID, err)
			}
			return
		}
	}
}

// handleTxnWrite wraps WriteHandler to use staging (Stage) instead of Add
// when inside a transaction. The staged deltas are promoted on COMMIT or
// discarded on ROLLBACK by TxnHandler.
func handleTxnWrite(
	wh *WriteHandler,
	_ *TxnHandler,
	clientConn net.Conn,
	rawPkt []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	// Set inTxn flag so WriteHandler methods use Stage instead of Add.
	wh.inTxn = true
	defer func() { wh.inTxn = false }()

	// For inserts (including REPLACE): delegate to the PK-aware handleInsert/handleReplace.
	if strategy == core.StrategyShadowWrite {
		if isReplaceInto(cl.RawSQL) {
			return wh.handleReplace(clientConn, rawPkt, cl)
		}
		return wh.handleInsert(clientConn, rawPkt, cl)
	}

	// For upserts (INSERT ... ON DUPLICATE KEY UPDATE inside a transaction):
	// delegate to handleUpsert which does its own hydration and delta tracking.
	if strategy == core.StrategyHydrateAndWrite && cl.SubType == core.SubInsert && cl.HasOnConflict {
		return wh.handleUpsert(clientConn, rawPkt, cl)
	}

	// For updates: hydrate, forward, then stage.
	if strategy == core.StrategyHydrateAndWrite {
		if len(cl.PKs) == 0 {
			return wh.handleBulkUpdate(clientConn, rawPkt, cl)
		}
		for _, pk := range cl.PKs {
			if wh.deltaMap.IsDelta(pk.Table, pk.PK) {
				continue
			}
			if err := wh.hydrateRow(pk.Table, pkValuesFromSingle(wh.tables[pk.Table], pk.PK)); err != nil {
				if wh.verbose {
					log.Printf("[conn %d] txn hydration failed for (%s, %s): %v", wh.connID, pk.Table, pk.PK, err)
				}
			}
		}
		if err := forwardAndRelay(rawPkt, wh.shadowConn, clientConn); err != nil {
			return err
		}
		for _, pk := range cl.PKs {
			wh.deltaMap.Stage(pk.Table, pk.PK)
		}
		return nil
	}

	// For deletes: forward, then stage tombstones.
	if strategy == core.StrategyShadowDelete {
		if len(cl.PKs) == 0 {
			return wh.handleBulkDelete(clientConn, rawPkt, cl)
		}
		if err := forwardAndRelay(rawPkt, wh.shadowConn, clientConn); err != nil {
			return err
		}
		for _, pk := range cl.PKs {
			wh.tombstones.Stage(pk.Table, pk.PK)
		}
		return nil
	}

	return wh.HandleWrite(clientConn, rawPkt, cl, strategy)
}

// classifyAndRoute determines which backend should handle a query.
func (p *Proxy) classifyAndRoute(sql string, connID int64) routeDecision {
	if sql == "" {
		return routeDecision{target: targetProd}
	}

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

	if p.verbose {
		log.Printf("[conn %d] %s/%s tables=%v → %s | %s",
			connID, classification.OpType, classification.SubType,
			classification.Tables, strategy, truncateSQL(sql, 100))
	}

	p.logger.Query(connID, sql, classification, strategy, 0)

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

	case core.StrategyTruncate:
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyForwardBoth:
		return routeDecision{
			target:         targetBoth,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyNotSupported:
		return routeDecision{
			target:         targetProd, // will be intercepted in loop
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyProdDirect:
		return routeDecision{target: targetProd, classification: classification, strategy: strategy}

	default:
		return routeDecision{target: targetProd, classification: classification, strategy: core.StrategyNotSupported}
	}
}
