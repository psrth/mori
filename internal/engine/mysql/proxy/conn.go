package proxy

import (
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/mori-dev/mori/internal/core"
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

	// Relay handshake between client and prod.
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

		// COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_CLOSE:
		// Pass through to prod for v1. (Prepared statement support is a future enhancement.)
		if cmd == comStmtPrepare || cmd == comStmtExecute || cmd == comStmtClose {
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
		// For v1, fall back to prod for merged reads.
		return routeDecision{
			target:         targetProd,
			classification: classification,
			strategy:       strategy,
		}

	default:
		return routeDecision{target: targetProd, strategy: strategy}
	}
}
