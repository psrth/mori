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
			prodConn:   prodConn,
			shadowConn: shadowConn,
			deltaMap:   p.deltaMap,
			tombstones: p.tombstones,
			tables:     p.tables,
			moriDir:    p.moriDir,
			connID:     connID,
			verbose:    p.verbose,
			logger:     p.logger,
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

			// Dispatch write strategies to WriteHandler.
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

		// RPC requests: pass through to prod (prepared statements).
		if pkt.Type == typeRPC {
			var allRaw []byte
			allRaw = append(allRaw, pkt.Raw...)
			for pkt.Status&statusEOM == 0 {
				pkt, err = readTDSPacket(clientConn)
				if err != nil {
					return
				}
				allRaw = append(allRaw, pkt.Raw...)
			}
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

	default:
		return routeDecision{target: targetProd, strategy: strategy}
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
	}

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
