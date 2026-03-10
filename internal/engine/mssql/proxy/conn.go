package proxy

import (
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mori-dev/mori/internal/core"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
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
		errPkt := buildErrorResponse("shadow database unavailable, refusing connection to protect production")
		clientConn.Write(errPkt)
		clientConn.Close()
		prodConn.Close()
		return
	}

	// Relay TDS handshake between client and prod.
	if err := relayHandshake(clientConn, prodConn); err != nil {
		log.Printf("[conn %d] handshake failed: %v", connID, err)
		clientConn.Close()
		prodConn.Close()
		return
	}

	// Connect to shadow.
	shadowConn, err := connectShadow(p.shadowAddr, p.shadowDBName)
	if err != nil {
		log.Printf("[conn %d] WRITE GUARD: shadow connection failed, refusing: %v", connID, err)
		errPkt := buildErrorResponse("shadow database unavailable, refusing connection to protect production")
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

	// Create a WriteHandler for this connection.
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

	// Link WriteHandler to TxnHandler for inTxn() checks.
	if wh != nil && txh != nil {
		wh.txnHandler = txh
	}

	// Create FKEnforcer if schema registry is available.
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

	// Create an ExtHandler for TDS RPC packets (sp_executesql, sp_prepare, etc.).
	var exth *ExtHandler
	if p.classifier != nil && p.router != nil {
		exth = &ExtHandler{
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
			stmtCache:      make(map[int32]string),
			writeHandler:   wh,
			readHandler:    rh,
		}
	}

	for {
		pkt, err := readTDSPacket(clientConn)
		if err != nil {
			if err != io.EOF && p.verbose {
				log.Printf("[conn %d] client read error: %v", connID, err)
			}
			return
		}

		// Handle attention signal (cancel).
		if pkt.Type == typeAttention {
			prodConn.Write(pkt.Raw)
			shadowConn.Write(pkt.Raw)
			// Drain any response from prod.
			drainTDSResponse(prodConn)
			continue
		}

		// SQL_BATCH: classify and route.
		if pkt.Type == typeSQLBatch {
			// If this is not EOM, collect remaining packets.
			var allRaw []byte
			allRaw = append(allRaw, pkt.Raw...)
			fullPayload := make([]byte, len(pkt.Payload))
			copy(fullPayload, pkt.Payload)

			for pkt.Status&statusEOM == 0 {
				pkt, err = readTDSPacket(clientConn)
				if err != nil {
					if p.verbose {
						log.Printf("[conn %d] client read error (continuation): %v", connID, err)
					}
					return
				}
				allRaw = append(allRaw, pkt.Raw...)
				fullPayload = append(fullPayload, pkt.Payload...)
			}

			sql := extractSQLFromBatch(fullPayload)
			decision := p.classifyAndRoute(sql, connID)

			// Dispatch transaction control to TxnHandler.
			if txh != nil && decision.classification != nil && decision.strategy == core.StrategyTransaction {
				if err := txh.HandleTxn(clientConn, allRaw, decision.classification); err != nil {
					if p.verbose {
						log.Printf("[conn %d] txn handler error: %v", connID, err)
					}
					return
				}
				continue
			}

			// P3 §4.5: StrategyNotSupported — return error to client.
			if decision.strategy == core.StrategyNotSupported && decision.classification != nil {
				msg := decision.classification.NotSupportedMsg
				if msg == "" {
					msg = core.UnsupportedTransactionMsg
				}
				errPkt := buildErrorResponse(msg)
				clientConn.Write(errPkt)
				continue
			}

			// P1 §2.1: StrategyTruncate — forward to Shadow, mark table fully shadowed.
			if decision.strategy == core.StrategyTruncate && decision.classification != nil {
				if err := forwardAndRelay(allRaw, shadowConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] truncate handler error: %v", connID, err)
					}
					return
				}
				for _, table := range decision.classification.Tables {
					if p.schemaRegistry != nil {
						p.schemaRegistry.MarkFullyShadowed(table)
					}
					if p.deltaMap != nil {
						p.deltaMap.ClearTable(table)
					}
					if p.tombstones != nil {
						p.tombstones.ClearTable(table)
					}
					if p.verbose {
						log.Printf("[conn %d] TRUNCATE: %s marked fully shadowed", connID, table)
					}
				}
				if p.schemaRegistry != nil {
					if err := coreSchema.WriteRegistry(p.moriDir, p.schemaRegistry); err != nil {
						if p.verbose {
							log.Printf("[conn %d] failed to persist schema registry after TRUNCATE: %v", connID, err)
						}
					}
				}
				continue
			}

			// Dispatch write strategies to WriteHandler.
			// The WriteHandler itself handles inTxn() staging via its txnHandler reference.
			if wh != nil && decision.classification != nil {
				switch decision.strategy {
				case core.StrategyShadowWrite,
					core.StrategyHydrateAndWrite,
					core.StrategyShadowDelete:
					if err := wh.HandleWrite(clientConn, allRaw, decision.classification, decision.strategy); err != nil {
						if p.verbose {
							log.Printf("[conn %d] write handler error: %v", connID, err)
						}
						return
					}
					continue
				}
			}

			// Dispatch merged read strategies to ReadHandler.
			if rh != nil && decision.classification != nil {
				switch decision.strategy {
				case core.StrategyMergedRead,
					core.StrategyJoinPatch:
					if err := rh.HandleRead(clientConn, allRaw, fullPayload, decision.classification, decision.strategy); err != nil {
						if p.verbose {
							log.Printf("[conn %d] read handler error: %v", connID, err)
						}
						return
					}
					continue
				}
			}

			switch decision.target {
			case targetProd:
				// WRITE GUARD L3: final check before prod dispatch.
				if decision.classification != nil &&
					(decision.classification.OpType == core.OpWrite || decision.classification.OpType == core.OpDDL) {
					log.Printf("[CRITICAL] [conn %d] WRITE GUARD L3: %s/%s reached targetProd — BLOCKED",
						connID, decision.classification.OpType, decision.classification.SubType)
					errPkt := buildErrorResponse("write operation blocked — internal routing error detected")
					clientConn.Write(errPkt)
					continue
				}
				if err := forwardAndRelay(allRaw, prodConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] prod relay error: %v", connID, err)
					}
					return
				}

			case targetShadow:
				if err := forwardAndRelay(allRaw, shadowConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] shadow relay error: %v", connID, err)
					}
					return
				}
				// Track DDL effects in the schema registry.
				if decision.strategy == core.StrategyShadowDDL && decision.classification != nil {
					p.trackDDLEffects(decision.classification, connID)
				}

			case targetBoth:
				// Send to shadow first, drain response.
				shadowConn.Write(allRaw)
				if err := drainTDSResponse(shadowConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] shadow drain error: %v", connID, err)
					}
				}
				// Forward to prod and relay response.
				if err := forwardAndRelay(allRaw, prodConn, clientConn); err != nil {
					if p.verbose {
						log.Printf("[conn %d] prod relay error (both): %v", connID, err)
					}
					return
				}
			}
			continue
		}

		// RPC requests: try ExtHandler, fall back to pass-through.
		if pkt.Type == typeRPC {
			var allRaw []byte
			allRaw = append(allRaw, pkt.Raw...)
			fullPayload := make([]byte, len(pkt.Payload))
			copy(fullPayload, pkt.Payload)

			for pkt.Status&statusEOM == 0 {
				pkt, err = readTDSPacket(clientConn)
				if err != nil {
					return
				}
				allRaw = append(allRaw, pkt.Raw...)
				fullPayload = append(fullPayload, pkt.Payload...)
			}

			if exth != nil {
				handled, extErr := exth.HandleRPC(clientConn, allRaw, fullPayload)
				if extErr != nil {
					if p.verbose {
						log.Printf("[conn %d] ext handler error: %v", connID, extErr)
					}
					return
				}
				if handled {
					continue
				}
			}

			// Unrecognized RPC — pass through to Prod.
			if err := forwardAndRelay(allRaw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] RPC relay error: %v", connID, err)
				}
				return
			}
			continue
		}

		// Transaction manager requests: forward to both.
		if pkt.Type == typeTransMgrRequest {
			var allRaw []byte
			allRaw = append(allRaw, pkt.Raw...)
			for pkt.Status&statusEOM == 0 {
				pkt, err = readTDSPacket(clientConn)
				if err != nil {
					return
				}
				allRaw = append(allRaw, pkt.Raw...)
			}
			shadowConn.Write(allRaw)
			drainTDSResponse(shadowConn)
			if err := forwardAndRelay(allRaw, prodConn, clientConn); err != nil {
				if p.verbose {
					log.Printf("[conn %d] txn mgr relay error: %v", connID, err)
				}
				return
			}
			continue
		}

		// Unknown packet type: forward to prod.
		if err := forwardAndRelay(pkt.Raw, prodConn, clientConn); err != nil {
			if p.verbose {
				log.Printf("[conn %d] unknown pkt relay error: %v", connID, err)
			}
			return
		}
	}
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
		log.Printf("[conn %d] %s/%s tables=%v -> %s | %s",
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

	case core.StrategyNotSupported:
		return routeDecision{
			target:         targetShadow, // won't actually be sent — handled before dispatch
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyProdDirect:
		return routeDecision{target: targetProd, classification: classification, strategy: strategy}

	default:
		return routeDecision{target: targetProd, classification: classification, strategy: core.StrategyNotSupported}
	}
}

// forwardAndRelay sends raw data to the backend and relays the complete TDS response
// back to the client.
func forwardAndRelay(raw []byte, backend, client net.Conn) error {
	if _, err := backend.Write(raw); err != nil {
		return fmt.Errorf("sending to backend: %w", err)
	}

	return relayTDSResponse(backend, client)
}

// relayTDSResponse reads a complete TDS response from backend and writes to client.
// A complete TDS response consists of one or more tabular result packets ending with EOM.
func relayTDSResponse(backend, client net.Conn) error {
	for {
		pkt, err := readTDSPacket(backend)
		if err != nil {
			return fmt.Errorf("reading backend response: %w", err)
		}

		if _, err := client.Write(pkt.Raw); err != nil {
			return fmt.Errorf("relaying to client: %w", err)
		}

		// EOM indicates end of the response message.
		if pkt.Status&statusEOM != 0 {
			return nil
		}
	}
}

// drainTDSResponse reads and discards a complete TDS response.
func drainTDSResponse(conn net.Conn) error {
	for {
		pkt, err := readTDSPacket(conn)
		if err != nil {
			return err
		}
		if pkt.Status&statusEOM != 0 {
			return nil
		}
	}
}

// trackDDLEffects updates the schema registry after a DDL statement executes on Shadow.
func (p *Proxy) trackDDLEffects(cl *core.Classification, connID int64) {
	if p.schemaRegistry == nil {
		return
	}
	sqlStr := cl.RawSQL
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	switch {
	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, " ADD "):
		table, col, colType := parseMSSQLAlterAddColumn(sqlStr)
		if table != "" && col != "" {
			p.schemaRegistry.RecordAddColumn(table, coreSchema.Column{Name: col, Type: colType})
			if p.verbose {
				log.Printf("[conn %d] schema registry: ADD COLUMN %s.%s (%s)", connID, table, col, colType)
			}
		}

	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "DROP COLUMN"):
		table, col := parseMSSQLAlterDropColumn(sqlStr)
		if table != "" && col != "" {
			p.schemaRegistry.RecordDropColumn(table, col)
			if p.verbose {
				log.Printf("[conn %d] schema registry: DROP COLUMN %s.%s", connID, table, col)
			}
		}

	case strings.HasPrefix(upper, "CREATE TABLE"):
		for _, table := range cl.Tables {
			p.schemaRegistry.RecordNewTable(table)
			if p.verbose {
				log.Printf("[conn %d] schema registry: CREATE TABLE %s", connID, table)
			}
		}

	case strings.HasPrefix(upper, "DROP TABLE"):
		for _, table := range cl.Tables {
			p.schemaRegistry.RemoveTable(table)
			if p.deltaMap != nil {
				p.deltaMap.ClearTable(table)
			}
			if p.tombstones != nil {
				p.tombstones.ClearTable(table)
			}
			if p.verbose {
				log.Printf("[conn %d] schema registry: DROP TABLE %s", connID, table)
			}
		}

	// P1 §2.7: sp_rename column tracking.
	case reSpRename.MatchString(sqlStr):
		table, oldName, newName := parseMSSQLSpRename(sqlStr)
		if table != "" && oldName != "" && newName != "" {
			p.schemaRegistry.RecordRenameColumn(table, oldName, newName)
			if p.verbose {
				log.Printf("[conn %d] schema registry: RENAME COLUMN %s.%s -> %s", connID, table, oldName, newName)
			}
		}

	// P3 §4.3: ALTER TYPE tracking.
	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ALTER COLUMN"):
		table, col, newType := parseMSSQLAlterColumn(sqlStr)
		if table != "" && col != "" {
			p.schemaRegistry.RecordTypeChange(table, col, "", newType)
			if p.verbose {
				log.Printf("[conn %d] schema registry: ALTER COLUMN %s.%s -> %s", connID, table, col, newType)
			}
		}
	}

	// P3 §4.4: Extract FK constraints from DDL before persisting.
	// When DDL contains ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES,
	// extract the FK metadata and store in schema registry.
	p.extractFKFromDDL(cl, connID)

	// Persist the registry.
	if err := coreSchema.WriteRegistry(p.moriDir, p.schemaRegistry); err != nil {
		if p.verbose {
			log.Printf("[conn %d] failed to persist schema registry: %v", connID, err)
		}
	}
}

// Regex for ALTER TABLE ... ADD column_name type
var reAlterAdd = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+\[?(\w+)\]?\s+ADD\s+\[?(\w+)\]?\s+(\w[\w\(\),\s]*)`)

// parseMSSQLAlterAddColumn parses "ALTER TABLE t ADD col TYPE" for MSSQL.
func parseMSSQLAlterAddColumn(sql string) (table, col, colType string) {
	m := reAlterAdd.FindStringSubmatch(sql)
	if len(m) < 4 {
		return "", "", ""
	}
	table = strings.ToLower(strings.Trim(m[1], "[]"))
	col = strings.ToLower(strings.Trim(m[2], "[]"))
	colType = strings.TrimSpace(m[3])
	// Trim trailing keywords like DEFAULT, NOT NULL, etc.
	for _, kw := range []string{"DEFAULT", "NOT NULL", "NULL", "CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "REFERENCES"} {
		if idx := strings.Index(strings.ToUpper(colType), kw); idx > 0 {
			colType = strings.TrimSpace(colType[:idx])
		}
	}
	return table, col, colType
}

// Regex for ALTER TABLE ... DROP COLUMN column_name
var reAlterDrop = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+\[?(\w+)\]?\s+DROP\s+COLUMN\s+\[?(\w+)\]?`)

// parseMSSQLAlterDropColumn parses "ALTER TABLE t DROP COLUMN col" for MSSQL.
func parseMSSQLAlterDropColumn(sql string) (table, col string) {
	m := reAlterDrop.FindStringSubmatch(sql)
	if len(m) < 3 {
		return "", ""
	}
	return strings.ToLower(strings.Trim(m[1], "[]")), strings.ToLower(strings.Trim(m[2], "[]"))
}

// Regex for sp_rename 't.old_col', 'new_col', 'COLUMN'
var reSpRename = regexp.MustCompile(`(?i)(?:EXEC(?:UTE)?\s+)?SP_RENAME\s+'([^']+)'\s*,\s*'([^']+)'\s*,\s*'COLUMN'`)

// parseMSSQLSpRename parses "EXEC sp_rename 't.old', 'new', 'COLUMN'" for column renaming.
func parseMSSQLSpRename(sql string) (table, oldName, newName string) {
	m := reSpRename.FindStringSubmatch(sql)
	if len(m) < 3 {
		return "", "", ""
	}
	// First arg is "table.old_col" or "schema.table.old_col".
	parts := strings.Split(m[1], ".")
	if len(parts) == 2 {
		table = strings.ToLower(strings.Trim(parts[0], "[]"))
		oldName = strings.ToLower(strings.Trim(parts[1], "[]"))
	} else if len(parts) == 3 {
		table = strings.ToLower(strings.Trim(parts[1], "[]"))
		oldName = strings.ToLower(strings.Trim(parts[2], "[]"))
	} else {
		return "", "", ""
	}
	newName = strings.ToLower(strings.Trim(m[2], "[]"))
	return table, oldName, newName
}

// Regex for ALTER TABLE ... ALTER COLUMN col newtype
var reAlterColumn = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+\[?(\w+)\]?\s+ALTER\s+COLUMN\s+\[?(\w+)\]?\s+(\w[\w\(\),\s]*)`)

// P3 §4.4: Regex for ADD CONSTRAINT ... FOREIGN KEY (...) REFERENCES ... (...)
var reAddForeignKey = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+\[?(\w+)\]?\s+ADD\s+CONSTRAINT\s+\[?(\w+)\]?\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+\[?(\w+)\]?\s*\(([^)]+)\)`)

// Regex for ON DELETE / ON UPDATE actions in FK DDL.
var reOnDelete = regexp.MustCompile(`(?i)\bON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION|RESTRICT)`)
var reOnUpdate = regexp.MustCompile(`(?i)\bON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION|RESTRICT)`)

// P3 §4.4: Regex for CREATE TABLE inline FK constraints.
var reInlineForeignKey = regexp.MustCompile(`(?i)CONSTRAINT\s+\[?(\w+)\]?\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+\[?(\w+)\]?\s*\(([^)]+)\)`)

// parseMSSQLAlterColumn parses "ALTER TABLE t ALTER COLUMN c newtype" for type changes.
func parseMSSQLAlterColumn(sql string) (table, col, newType string) {
	m := reAlterColumn.FindStringSubmatch(sql)
	if len(m) < 4 {
		return "", "", ""
	}
	table = strings.ToLower(strings.Trim(m[1], "[]"))
	col = strings.ToLower(strings.Trim(m[2], "[]"))
	newType = strings.TrimSpace(m[3])
	// Trim trailing keywords.
	for _, kw := range []string{"NOT NULL", "NULL", "COLLATE"} {
		if idx := strings.Index(strings.ToUpper(newType), kw); idx > 0 {
			newType = strings.TrimSpace(newType[:idx])
		}
	}
	return table, col, newType
}

// ---------------------------------------------------------------------------
// P3 §4.4 — FK Extraction from DDL
// ---------------------------------------------------------------------------

// extractFKFromDDL extracts foreign key constraints from DDL statements and
// stores them in the schema registry. This handles:
// - ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (cols) REFERENCES parent(cols)
// - CREATE TABLE with inline CONSTRAINT ... FOREIGN KEY ... REFERENCES
func (p *Proxy) extractFKFromDDL(cl *core.Classification, connID int64) {
	if p.schemaRegistry == nil {
		return
	}

	sqlStr := cl.RawSQL
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	// Handle ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES.
	if strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "FOREIGN KEY") {
		if m := reAddForeignKey.FindStringSubmatch(sqlStr); len(m) >= 6 {
			childTable := strings.ToLower(strings.Trim(m[1], "[]"))
			constraintName := strings.ToLower(strings.Trim(m[2], "[]"))
			childCols := parseColumnList(m[3])
			parentTable := strings.ToLower(strings.Trim(m[4], "[]"))
			parentCols := parseColumnList(m[5])

			onDelete := "NO ACTION"
			if dm := reOnDelete.FindStringSubmatch(sqlStr); len(dm) > 1 {
				onDelete = strings.ToUpper(strings.TrimSpace(dm[1]))
			}
			onUpdate := "NO ACTION"
			if um := reOnUpdate.FindStringSubmatch(sqlStr); len(um) > 1 {
				onUpdate = strings.ToUpper(strings.TrimSpace(um[1]))
			}

			fk := coreSchema.ForeignKey{
				ConstraintName: constraintName,
				ChildTable:     childTable,
				ChildColumns:   childCols,
				ParentTable:    parentTable,
				ParentColumns:  parentCols,
				OnDelete:       onDelete,
				OnUpdate:       onUpdate,
			}
			p.schemaRegistry.RecordForeignKey(childTable, fk)

			if p.verbose {
				log.Printf("[conn %d] schema registry: FK %s on %s(%v) -> %s(%v) [DELETE=%s UPDATE=%s]",
					connID, constraintName, childTable, childCols, parentTable, parentCols, onDelete, onUpdate)
			}
		}
	}

	// Handle CREATE TABLE with inline FK constraints.
	if strings.HasPrefix(upper, "CREATE TABLE") && strings.Contains(upper, "FOREIGN KEY") {
		childTable := ""
		if len(cl.Tables) > 0 {
			childTable = cl.Tables[0]
		}
		if childTable == "" {
			return
		}

		for _, m := range reInlineForeignKey.FindAllStringSubmatch(sqlStr, -1) {
			if len(m) < 5 {
				continue
			}
			constraintName := strings.ToLower(strings.Trim(m[1], "[]"))
			childCols := parseColumnList(m[2])
			parentTable := strings.ToLower(strings.Trim(m[3], "[]"))
			parentCols := parseColumnList(m[4])

			// Look for ON DELETE / ON UPDATE after this constraint match.
			onDelete := "NO ACTION"
			onUpdate := "NO ACTION"
			matchEnd := strings.Index(sqlStr, m[0]) + len(m[0])
			if matchEnd < len(sqlStr) {
				remainder := sqlStr[matchEnd:]
				if dm := reOnDelete.FindStringSubmatch(remainder); len(dm) > 1 {
					onDelete = strings.ToUpper(strings.TrimSpace(dm[1]))
				}
				if um := reOnUpdate.FindStringSubmatch(remainder); len(um) > 1 {
					onUpdate = strings.ToUpper(strings.TrimSpace(um[1]))
				}
			}

			fk := coreSchema.ForeignKey{
				ConstraintName: constraintName,
				ChildTable:     childTable,
				ChildColumns:   childCols,
				ParentTable:    parentTable,
				ParentColumns:  parentCols,
				OnDelete:       onDelete,
				OnUpdate:       onUpdate,
			}
			p.schemaRegistry.RecordForeignKey(childTable, fk)

			if p.verbose {
				log.Printf("[conn %d] schema registry: FK (inline) %s on %s(%v) -> %s(%v)",
					connID, constraintName, childTable, childCols, parentTable, parentCols)
			}
		}
	}
}

// parseColumnList splits a comma-separated column list and cleans each name.
func parseColumnList(s string) []string {
	parts := strings.Split(s, ",")
	var cols []string
	for _, part := range parts {
		col := strings.TrimSpace(part)
		col = strings.Trim(col, "[]`\"")
		col = strings.ToLower(col)
		if col != "" {
			cols = append(cols, col)
		}
	}
	return cols
}
