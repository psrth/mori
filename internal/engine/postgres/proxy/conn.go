package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/mori-dev/mori/internal/core"
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

	// If routing is not available, fall back to relay mode.
	if !p.canRoute() {
		p.relayConn(clientConn, prodConn, connID)
		return
	}

	// Perform the startup handshake and connect to Shadow.
	shadowConn, err := p.performStartup(clientConn, prodConn, connID)
	if err != nil || shadowConn == nil {
		if p.verbose && err != nil {
			log.Printf("[conn %d] Shadow handshake failed, degrading to relay: %v", connID, err)
		}
		p.relayConn(clientConn, prodConn, connID)
		return
	}

	p.routeLoop(clientConn, prodConn, shadowConn, connID)
}

// relayConn is the Phase 3 fallback: bidirectional io.Copy between client and Prod.
func (p *Proxy) relayConn(clientConn, prodConn net.Conn, connID int64) {
	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			prodConn.Close()
		})
	}
	defer closeBoth()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, err := io.Copy(prodConn, clientConn)
		if p.verbose && err != nil {
			log.Printf("[conn %d] client→prod: %v", connID, err)
		}
		closeBoth()
	}()

	go func() {
		defer wg.Done()
		_, err := io.Copy(clientConn, prodConn)
		if p.verbose && err != nil {
			log.Printf("[conn %d] prod→client: %v", connID, err)
		}
		closeBoth()
	}()

	wg.Wait()

	if p.verbose {
		log.Printf("[conn %d] closed (relay mode)", connID)
	}
}

// performStartup handles the PG startup handshake phase.
// It relays the client's startup sequence to Prod (transparent to client),
// and independently initiates a Shadow connection.
func (p *Proxy) performStartup(clientConn, prodConn net.Conn, connID int64) (net.Conn, error) {
	if err := relayStartup(clientConn, prodConn); err != nil {
		return nil, fmt.Errorf("prod startup: %w", err)
	}

	shadowConn, err := p.connectShadow(connID)
	if err != nil {
		return nil, fmt.Errorf("shadow startup: %w", err)
	}
	return shadowConn, nil
}

// relayStartup relays the PG startup handshake between client and Prod.
// Handles SSLRequest, StartupMessage, auth exchanges, and waits for ReadyForQuery.
func relayStartup(clientConn, prodConn net.Conn) error {
	// Read initial message from client (no type byte).
	startupRaw, err := readStartupMsg(clientConn)
	if err != nil {
		return fmt.Errorf("reading startup: %w", err)
	}

	// Handle SSLRequest.
	if isSSLRequest(startupRaw) {
		if _, err := prodConn.Write(startupRaw); err != nil {
			return fmt.Errorf("forwarding SSLRequest: %w", err)
		}
		var sslResp [1]byte
		if _, err := io.ReadFull(prodConn, sslResp[:]); err != nil {
			return fmt.Errorf("reading SSL response: %w", err)
		}
		if _, err := clientConn.Write(sslResp[:]); err != nil {
			return fmt.Errorf("relaying SSL response: %w", err)
		}
		// After SSLRequest, client sends the real StartupMessage.
		startupRaw, err = readStartupMsg(clientConn)
		if err != nil {
			return fmt.Errorf("reading startup after SSL: %w", err)
		}
	}

	// Forward StartupMessage to Prod.
	if _, err := prodConn.Write(startupRaw); err != nil {
		return fmt.Errorf("forwarding startup: %w", err)
	}

	// Relay messages from Prod to client until ReadyForQuery ('Z').
	for {
		msg, err := readMsg(prodConn)
		if err != nil {
			return fmt.Errorf("reading prod startup response: %w", err)
		}

		if _, err := clientConn.Write(msg.Raw); err != nil {
			return fmt.Errorf("relaying to client: %w", err)
		}

		if msg.Type == 'Z' {
			return nil
		}

		// If Prod sent an auth request, the client may need to respond.
		if msg.Type == 'R' && len(msg.Payload) >= 4 {
			authType := binary.BigEndian.Uint32(msg.Payload[:4])
			if authType != 0 { // 0 = AuthenticationOk
				clientMsg, err := readMsg(clientConn)
				if err != nil {
					return fmt.Errorf("reading client auth response: %w", err)
				}
				if _, err := prodConn.Write(clientMsg.Raw); err != nil {
					return fmt.Errorf("forwarding client auth: %w", err)
				}
			}
		}
	}
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
			prodConn:   prodConn,
			shadowConn: shadowConn,
			deltaMap:   p.deltaMap,
			tombstones: p.tombstones,
			moriDir:    p.moriDir,
			connID:     connID,
			verbose:    p.verbose,
			logger:     p.logger,
		}
	}

	// Link TxnHandler to WriteHandler for staged delta awareness.
	if wh != nil && txh != nil {
		wh.txnHandler = txh
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

		switch decision.target {
		case targetProd:
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

	default:
		// ProdDirect, Other
		return routeDecision{target: targetProd, strategy: strategy}
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
