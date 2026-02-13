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

	prodConn, err := net.DialTimeout("tcp", p.prodAddr, 10*time.Second)
	if err != nil {
		log.Printf("[conn %d] failed to connect to prod %s: %v", connID, p.prodAddr, err)
		clientConn.Close()
		return
	}

	// WRITE GUARD: if routing is not available, refuse the connection.
	if !p.canRoute() {
		log.Printf("[conn %d] WRITE GUARD: shadow unavailable, refusing connection", connID)
		refuse := buildTNSRefuse("shadow database unavailable, refusing connection to protect production")
		clientConn.Write(refuse)
		clientConn.Close()
		prodConn.Close()
		return
	}

	// Relay TNS handshake between client and prod.
	if err := relayTNSHandshake(clientConn, prodConn); err != nil {
		log.Printf("[conn %d] TNS handshake failed: %v", connID, err)
		clientConn.Close()
		prodConn.Close()
		return
	}

	guardedProd := NewSafeProdConn(prodConn, connID, p.verbose, p.logger)
	p.routeLoop(clientConn, guardedProd, connID)
}

// relayTNSHandshake relays the TNS connection handshake between client and production.
// TNS handshake flow:
// 1. Client -> Server: Connect packet
// 2. Server -> Client: Accept/Refuse/Redirect packet
// 3. If Redirect: follow redirect
// After Accept, authentication proceeds over Data packets.
func relayTNSHandshake(clientConn, prodConn net.Conn) error {
	// 1. Read Connect from client.
	connectPkt, err := readTNSPacket(clientConn)
	if err != nil {
		return err
	}

	// Forward to prod.
	if _, err := prodConn.Write(connectPkt.Raw); err != nil {
		return err
	}

	// 2. Read response from prod.
	for {
		respPkt, err := readTNSPacket(prodConn)
		if err != nil {
			return err
		}

		// Forward to client.
		if _, err := clientConn.Write(respPkt.Raw); err != nil {
			return err
		}

		switch respPkt.PacketType {
		case tnsAccept:
			return nil
		case tnsRefuse:
			return io.EOF
		case tnsRedirect:
			// Redirect: client sends a new Connect.
			newConnect, err := readTNSPacket(clientConn)
			if err != nil {
				return err
			}
			if _, err := prodConn.Write(newConnect.Raw); err != nil {
				return err
			}
			continue
		case tnsResend:
			// Server wants the Connect packet again.
			if _, err := prodConn.Write(connectPkt.Raw); err != nil {
				return err
			}
			continue
		default:
			// Other packet types during handshake — relay to client and continue.
			continue
		}
	}
}

// routeLoop is the main query routing loop for a connection.
// It relays TNS Data packets between client and prod, intercepting SQL for classification.
func (p *Proxy) routeLoop(clientConn, prodConn net.Conn, connID int64) {
	var closeOnce sync.Once
	closeAll := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			prodConn.Close()
		})
	}
	defer closeAll()

	// After the TNS handshake, authentication and subsequent communication happens
	// over TNS Data packets. We relay all packets, inspecting Data packets for SQL.
	//
	// For v1, we relay authentication transparently and classify SQL in Data packets.
	// Shadow queries are not wired up via raw TNS (would require full TNS auth);
	// instead, shadow routing would use the go-ora driver in a future enhancement.
	// For now, the proxy acts as a pass-through with write guard protection.

	// Start bidirectional relay with write guard.
	errCh := make(chan error, 2)

	// Client -> Prod (with classification and guard).
	go func() {
		for {
			pkt, err := readTNSPacket(clientConn)
			if err != nil {
				errCh <- err
				return
			}

			if pkt.PacketType == tnsData && len(pkt.Payload) > 0 {
				sql := extractQueryFromTNSData(pkt.Payload)
				if sql != "" {
					decision := p.classifyAndRoute(sql, connID)

					// WRITE GUARD L3: block writes to prod.
					if decision.target == targetProd &&
						decision.classification != nil &&
						(decision.classification.OpType == core.OpWrite || decision.classification.OpType == core.OpDDL) {
						log.Printf("[CRITICAL] [conn %d] WRITE GUARD L3: %s/%s reached targetProd — BLOCKED",
							connID, decision.classification.OpType, decision.classification.SubType)
						// Send a TNS Data packet with error back to client.
						errMsg := buildTNSRefuse("mori: write operation blocked — internal routing error detected")
						clientConn.Write(errMsg)
						continue
					}
				}
			}

			// Forward to prod (through SafeProdConn L2 guard).
			if _, err := prodConn.Write(pkt.Raw); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Prod -> Client (pass-through).
	go func() {
		buf := make([]byte, 32768)
		for {
			n, err := prodConn.Read(buf)
			if err != nil {
				errCh <- err
				return
			}
			if _, err := clientConn.Write(buf[:n]); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Wait for either direction to close.
	<-errCh
	if p.verbose {
		log.Printf("[conn %d] connection closed", connID)
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
