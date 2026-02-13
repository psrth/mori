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
	useV2, err := relayTNSHandshake(clientConn, prodConn)
	if err != nil {
		log.Printf("[conn %d] TNS handshake failed: %v", connID, err)
		clientConn.Close()
		prodConn.Close()
		return
	}

	guardedProd := NewSafeProdConn(prodConn, connID, p.verbose, p.logger)
	p.routeLoop(clientConn, guardedProd, connID, useV2)
}

// relayTNSHandshake relays the TNS connection handshake between client and production.
// TNS handshake flow:
// 1. Client -> Server: Connect packet (+ optional Data packet for large connect strings)
// 2. Server -> Client: Accept/Refuse/Redirect packet
// 3. If Redirect: follow redirect
// After Accept, authentication proceeds over Data packets.
func relayTNSHandshake(clientConn, prodConn net.Conn) (useV2 bool, err error) {
	// 1. Read Connect from client and forward to prod.
	connectPkt, connectDataPkt, err := relayConnectPackets(clientConn, prodConn)
	if err != nil {
		return false, err
	}

	// 2. Read response from prod.
	for {
		respPkt, err := readTNSPacket(prodConn)
		if err != nil {
			return false, err
		}

		switch respPkt.PacketType {
		case tnsAccept:
			// Forward Accept to client.
			if _, err := clientConn.Write(respPkt.Raw); err != nil {
				return false, err
			}
			// Parse negotiated TNS version from Accept payload.
			// Accept payload layout: version(2 BE) + service_options(2) + ...
			// When version >= 315, post-handshake packets use 4-byte lengths.
			if len(respPkt.Payload) >= 2 {
				version := binary.BigEndian.Uint16(respPkt.Payload[0:2])
				useV2 = version >= 315
				log.Printf("[handshake] Accept: TNS version=%d, useV2=%v, payload_len=%d", version, useV2, len(respPkt.Payload))
			}
			return useV2, nil
		case tnsRefuse:
			// Forward Refuse to client.
			if _, err := clientConn.Write(respPkt.Raw); err != nil {
				return false, err
			}
			return false, io.EOF
		case tnsRedirect:
			// Forward Redirect to client, then read new Connect.
			if _, err := clientConn.Write(respPkt.Raw); err != nil {
				return false, err
			}
			connectPkt, connectDataPkt, err = relayConnectPackets(clientConn, prodConn)
			if err != nil {
				return false, err
			}
			continue
		case tnsResend:
			// Server wants the Connect packet again. Do NOT forward the Resend
			// to the client — the go-ora driver would resend its packets into our
			// buffer, corrupting the post-handshake stream. Instead, replay our
			// saved copies directly to prod.
			if _, err := prodConn.Write(connectPkt.Raw); err != nil {
				return false, err
			}
			if connectDataPkt != nil {
				if _, err := prodConn.Write(connectDataPkt.Raw); err != nil {
					return false, err
				}
			}
			continue
		default:
			// Other packet types during handshake — relay to client and continue.
			if _, err := clientConn.Write(respPkt.Raw); err != nil {
				return false, err
			}
			continue
		}
	}
}

// relayConnectPackets reads a Connect packet from the client, forwards it to prod,
// and handles the two-packet Connect flow used by go-ora when the connect string
// exceeds 230 bytes. In that case the Connect packet is exactly 70 bytes (the fixed
// header size) and the connect data follows in a separate Data packet.
// Returns the Connect packet and the optional Data packet (nil if single-packet flow).
func relayConnectPackets(clientConn, prodConn net.Conn) (connectPkt, dataPkt *tnsMsg, err error) {
	connectPkt, err = readTNSPacket(clientConn)
	if err != nil {
		return nil, nil, err
	}

	if _, err = prodConn.Write(connectPkt.Raw); err != nil {
		return nil, nil, err
	}

	// go-ora sends connect data in a separate Data packet when the connect
	// string exceeds 230 bytes. The Connect packet is exactly 70 bytes (its
	// fixed header: 8-byte TNS header + 62-byte connect-specific fields).
	// We must relay that Data packet before prod will respond.
	if connectPkt.PacketType == tnsConnect && len(connectPkt.Raw) == 70 {
		dataPkt, err = readTNSPacket(clientConn)
		if err != nil {
			return nil, nil, fmt.Errorf("reading connect data packet: %w", err)
		}
		if _, err = prodConn.Write(dataPkt.Raw); err != nil {
			return nil, nil, err
		}
	}

	return connectPkt, dataPkt, nil
}

// routeLoop is the main query routing loop for a connection.
// It relays TNS Data packets between client and prod, intercepting SQL for classification.
// useV2 indicates post-handshake 4-byte length framing (TNS version >= 315).
func (p *Proxy) routeLoop(clientConn, prodConn net.Conn, connID int64, useV2 bool) {
	var closeOnce sync.Once
	closeAll := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			prodConn.Close()
		})
	}
	defer closeAll()

	// Select the correct packet reader for the negotiated TNS version.
	readPkt := readTNSPacket
	buildRefuse := buildTNSRefuse
	if useV2 {
		readPkt = readTNSPacketV2
		buildRefuse = buildTNSRefuseV2
	}
	if p.verbose {
		log.Printf("[conn %d] routeLoop started, useV2=%v", connID, useV2)
	}

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
			pkt, err := readPkt(clientConn)
			if err != nil {
				if p.verbose {
					log.Printf("[conn %d] client->prod read error: %v", connID, err)
				}
				errCh <- err
				return
			}
			if p.verbose {
				log.Printf("[conn %d] client->prod: type=%d len=%d", connID, pkt.PacketType, len(pkt.Raw))
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
						// Send a TNS Refuse packet with error back to client.
						errMsg := buildRefuse("mori: write operation blocked — internal routing error detected")
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
				if p.verbose {
					log.Printf("[conn %d] prod->client read error: %v", connID, err)
				}
				errCh <- err
				return
			}
			if p.verbose {
				log.Printf("[conn %d] prod->client: %d bytes", connID, n)
			}
			if _, err := clientConn.Write(buf[:n]); err != nil {
				if p.verbose {
					log.Printf("[conn %d] prod->client write error: %v", connID, err)
				}
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
