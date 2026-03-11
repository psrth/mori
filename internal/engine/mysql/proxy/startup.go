package proxy

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/psrth/mori/internal/core/tlsutil"
)

// clientSSL is the MySQL CLIENT_SSL capability flag.
const clientSSL uint32 = 0x0800

// clientDeprecateEOF is the MySQL CLIENT_DEPRECATE_EOF capability flag (bit 24).
// When set, the server replaces EOF packets with OK packets in result sets.
// The proxy relies on EOF packets for result-set framing, so this flag must
// be stripped from both server and client handshake packets.
const clientDeprecateEOF uint32 = 0x01000000

// relayHandshake relays the MySQL handshake between client and production server.
// MySQL handshake flow:
// 1. Server -> Client: Initial Handshake Packet
// 2. Client -> Server: Handshake Response Packet
// 3. Server -> Client: OK/ERR/AuthSwitch
// 4. If AuthSwitch: Client -> Server: auth data, Server -> Client: OK/ERR
//
// SSL handling: the proxy listens on localhost only, so it strips CLIENT_SSL
// from the server handshake before forwarding to the client. If the server
// supports SSL, the proxy independently negotiates TLS with prod.
func relayHandshake(clientConn, prodConn net.Conn, tlsParams tlsutil.TLSParams) (net.Conn, error) {
	// 1. Read initial handshake from Prod.
	handshake, err := readMySQLPacket(prodConn)
	if err != nil {
		return prodConn, fmt.Errorf("reading prod handshake: %w", err)
	}

	// Check if Prod supports SSL and strip the flag before forwarding to client.
	prodSupportsSSL := stripSSLCapability(handshake.Payload)

	// Rebuild raw packet with modified payload.
	clientHandshake := buildMySQLPacket(handshake.Sequence, handshake.Payload)
	if _, err := clientConn.Write(clientHandshake); err != nil {
		return prodConn, fmt.Errorf("forwarding handshake to client: %w", err)
	}

	// If SSLMode is "disable", skip SSL negotiation entirely.
	if tlsParams.SSLMode == "disable" {
		// Do not negotiate SSL regardless of prod capabilities.
	} else if prodSupportsSSL {
		// Prod supports SSL; negotiate TLS before the client sends its response.
		charset := handshakeCharset(handshake.Payload)
		prodConn, err = negotiateMySQLSSL(prodConn, charset, tlsParams)
		if err != nil {
			return prodConn, fmt.Errorf("prod SSL negotiation: %w", err)
		}
	} else if tlsParams.SSLMode == "prefer" {
		// "prefer" falls back to plaintext when the server doesn't support SSL.
	} else if tlsParams.SSLMode == "require" || tlsParams.SSLMode == "verify-ca" || tlsParams.SSLMode == "verify-full" {
		// SSL is required but prod does not support it.
		return prodConn, fmt.Errorf("sslmode %q requires SSL but prod server does not support it", tlsParams.SSLMode)
	}

	// 2. Read handshake response from client (plain TCP — client didn't see SSL).
	response, err := readMySQLPacket(clientConn)
	if err != nil {
		return prodConn, fmt.Errorf("reading client handshake response: %w", err)
	}

	// Strip CLIENT_DEPRECATE_EOF from the client's capability flags so the
	// server always sends EOF-terminated result sets, which the proxy expects.
	stripClientDeprecateEOF(response.Payload)

	// Rebuild the raw packet since the payload was modified.
	response.Raw = buildMySQLPacket(response.Sequence, response.Payload)

	// Forward to Prod (possibly over TLS).
	if _, err := prodConn.Write(response.Raw); err != nil {
		return prodConn, fmt.Errorf("forwarding handshake response to prod: %w", err)
	}

	// 3. Read auth result from Prod.
	// May need multiple rounds for auth switch.
	for {
		authResult, err := readMySQLPacket(prodConn)
		if err != nil {
			return prodConn, fmt.Errorf("reading prod auth result: %w", err)
		}

		// Forward to client.
		if _, err := clientConn.Write(authResult.Raw); err != nil {
			return prodConn, fmt.Errorf("forwarding auth result to client: %w", err)
		}

		if len(authResult.Payload) == 0 {
			return prodConn, fmt.Errorf("empty auth result from prod")
		}

		switch authResult.Payload[0] {
		case iOK:
			return prodConn, nil
		case iERR:
			return prodConn, fmt.Errorf("prod auth error: %s", string(authResult.Payload[9:]))
		case iEOF:
			// 0xFE is both EOF marker and auth switch request.
			if len(authResult.Payload) == 1 {
				// Old-auth (EOF with no payload): client sends password.
				clientAuth, err := readMySQLPacket(clientConn)
				if err != nil {
					return prodConn, fmt.Errorf("reading client old-auth: %w", err)
				}
				if _, err := prodConn.Write(clientAuth.Raw); err != nil {
					return prodConn, fmt.Errorf("forwarding old-auth to prod: %w", err)
				}
				continue
			}
			// Auth switch request (0xFE with plugin name and auth data).
			// The packet was already forwarded to the client above;
			// now read the client's auth switch response and relay to prod.
			clientAuth, err := readMySQLPacket(clientConn)
			if err != nil {
				return prodConn, fmt.Errorf("reading client auth switch response: %w", err)
			}
			if _, err := prodConn.Write(clientAuth.Raw); err != nil {
				return prodConn, fmt.Errorf("forwarding auth switch response to prod: %w", err)
			}
			continue
		default:
			if authResult.Payload[0] == 0x01 {
				// Auth more data (caching_sha2_password).
				// 0x01 0x03 = fast auth success → server will send OK next, don't read from client.
				// 0x01 0x04 = full auth required → client will send encrypted password.
				if len(authResult.Payload) > 1 && authResult.Payload[1] == 0x03 {
					// Fast auth success: continue to read the final OK from server.
					continue
				}
				// Full auth or other: relay client's auth response.
				clientAuth, err := readMySQLPacket(clientConn)
				if err != nil {
					return prodConn, fmt.Errorf("reading client auth continuation: %w", err)
				}
				if _, err := prodConn.Write(clientAuth.Raw); err != nil {
					return prodConn, fmt.Errorf("forwarding auth continuation to prod: %w", err)
				}
				continue
			}
			return prodConn, nil
		}
	}
}

// stripSSLCapability clears the CLIENT_SSL (0x0800) flag and the
// CLIENT_DEPRECATE_EOF (0x01000000) flag from a MySQL Initial Handshake
// Packet (server → client) payload. Returns true if the SSL flag was
// originally set.
//
// CLIENT_DEPRECATE_EOF is stripped here so the client never sees the
// server advertising the capability — preventing the client from setting
// it in its HandshakeResponse41. However, a client may set the flag
// regardless of what the server advertises, so stripClientDeprecateEOF()
// must also be called on the client's response to guarantee the flag is
// cleared. Both strips are necessary for correct result-set framing.
func stripSSLCapability(payload []byte) bool {
	if len(payload) < 2 {
		return false
	}

	// Walk to the lower capability flags:
	// 1 byte protocol version + variable server version + 4 bytes conn ID +
	// 8 bytes auth data part 1 + 1 byte filler = offset to caps lower.
	pos := 1
	for pos < len(payload) && payload[pos] != 0 {
		pos++
	}
	pos++ // Skip null terminator.
	pos += 4 // Connection ID.
	pos += 8 // Auth data part 1.
	pos++    // Filler.

	if pos+2 > len(payload) {
		return false
	}

	capsLowerPos := pos

	// Lower capability flags (2 bytes LE).
	capsLower := binary.LittleEndian.Uint16(payload[capsLowerPos : capsLowerPos+2])
	hadSSL := capsLower&uint16(clientSSL) != 0
	if hadSSL {
		capsLower &^= uint16(clientSSL)
		binary.LittleEndian.PutUint16(payload[capsLowerPos:capsLowerPos+2], capsLower)
	}

	// Upper capability flags are at: capsLowerPos + 2 (lower caps) + 1 (charset) + 2 (status flags).
	capsUpperPos := capsLowerPos + 2 + 1 + 2
	if capsUpperPos+2 <= len(payload) {
		capsUpper := binary.LittleEndian.Uint16(payload[capsUpperPos : capsUpperPos+2])
		// CLIENT_DEPRECATE_EOF = 0x01000000; in upper 16 bits that is 0x0100.
		deprecateEOFUpper := uint16(clientDeprecateEOF >> 16)
		if capsUpper&deprecateEOFUpper != 0 {
			capsUpper &^= deprecateEOFUpper
			binary.LittleEndian.PutUint16(payload[capsUpperPos:capsUpperPos+2], capsUpper)
		}
	}

	return hadSSL
}

// stripClientDeprecateEOF clears the CLIENT_DEPRECATE_EOF flag from a MySQL
// HandshakeResponse41 payload. The capability flags are the first 4 bytes (LE).
func stripClientDeprecateEOF(payload []byte) {
	if len(payload) < 4 {
		return
	}
	caps := binary.LittleEndian.Uint32(payload[0:4])
	if caps&clientDeprecateEOF != 0 {
		caps &^= clientDeprecateEOF
		binary.LittleEndian.PutUint32(payload[0:4], caps)
	}
}

// handshakeCharset extracts the character set byte from a MySQL
// Initial Handshake Packet payload. Returns 0x21 (utf8) as default.
func handshakeCharset(payload []byte) byte {
	pos := 1
	for pos < len(payload) && payload[pos] != 0 {
		pos++
	}
	pos++ // Skip null.
	pos += 4 // Connection ID.
	pos += 8 // Auth data part 1.
	pos++    // Filler.
	pos += 2 // Capability flags lower.

	if pos >= len(payload) {
		return 0x21 // utf8 default
	}
	return payload[pos]
}

// negotiateMySQLSSL sends a MySQL SSL Request to the server and upgrades
// the connection to TLS. The SSL Request is a truncated Handshake Response
// containing only capability flags, max packet size, charset, and reserved bytes.
func negotiateMySQLSSL(prodConn net.Conn, charset byte, tlsParams tlsutil.TLSParams) (net.Conn, error) {
	// Build SSL Request payload (32 bytes):
	// caps(4 LE) + max_packet(4 LE) + charset(1) + reserved(23 zeros)
	var payload [32]byte
	// Capability flags: CLIENT_SSL | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION
	caps := clientSSL | 0x00000200 | 0x00008000
	binary.LittleEndian.PutUint32(payload[0:4], caps)
	binary.LittleEndian.PutUint32(payload[4:8], 16*1024*1024) // max packet
	payload[8] = charset

	if err := writeMySQLPacket(prodConn, 1, payload[:]); err != nil {
		return prodConn, fmt.Errorf("sending SSL request to prod: %w", err)
	}

	tlsCfg, err := tlsutil.BuildConfig(tlsParams)
	if err != nil {
		return prodConn, fmt.Errorf("building TLS config: %w", err)
	}
	if tlsCfg == nil {
		return prodConn, fmt.Errorf("TLS config is nil (SSLMode=%q); caller should not have initiated SSL negotiation", tlsParams.SSLMode)
	}

	tlsConn := tls.Client(prodConn, tlsCfg)
	if err := tlsConn.Handshake(); err != nil {
		return prodConn, fmt.Errorf("TLS handshake with prod: %w", err)
	}

	return tlsConn, nil
}

// connectShadow establishes a TCP connection to the Shadow MySQL database
// and performs the MySQL handshake with known credentials.
func connectShadow(shadowAddr, dbName string) (net.Conn, error) {
	shadowConn, err := net.DialTimeout("tcp", shadowAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial shadow %s: %w", shadowAddr, err)
	}

	// 1. Read Shadow's initial handshake.
	handshake, err := readMySQLPacket(shadowConn)
	if err != nil {
		shadowConn.Close()
		return nil, fmt.Errorf("reading shadow handshake: %w", err)
	}

	if len(handshake.Payload) < 1 {
		shadowConn.Close()
		return nil, fmt.Errorf("shadow handshake payload too short")
	}

	// Parse auth data from handshake for mysql_native_password.
	authData, authPlugin := parseHandshake(handshake.Payload)

	// 2. Build and send handshake response.
	responsePayload := buildHandshakeResponse("root", "mori", dbName, authData, authPlugin)
	if err := writeMySQLPacket(shadowConn, 1, responsePayload); err != nil {
		shadowConn.Close()
		return nil, fmt.Errorf("sending shadow handshake response: %w", err)
	}

	// 3. Read auth result.
	for {
		result, err := readMySQLPacket(shadowConn)
		if err != nil {
			shadowConn.Close()
			return nil, fmt.Errorf("reading shadow auth result: %w", err)
		}

		if len(result.Payload) == 0 {
			shadowConn.Close()
			return nil, fmt.Errorf("empty auth result from shadow")
		}

		switch result.Payload[0] {
		case iOK:
			return shadowConn, nil
		case iERR:
			shadowConn.Close()
			msg := ""
			if len(result.Payload) > 9 {
				msg = string(result.Payload[9:])
			}
			return nil, fmt.Errorf("shadow auth error: %s", msg)
		case 0xfe:
			// Auth switch request. Handle mysql_native_password.
			if len(result.Payload) > 1 {
				pluginEnd := 1
				for pluginEnd < len(result.Payload) && result.Payload[pluginEnd] != 0 {
					pluginEnd++
				}
				newAuthData := result.Payload[pluginEnd+1:]
				if len(newAuthData) > 0 && newAuthData[len(newAuthData)-1] == 0 {
					newAuthData = newAuthData[:len(newAuthData)-1]
				}
				authResp := scramblePassword(newAuthData, "mori")
				if err := writeMySQLPacket(shadowConn, result.Sequence+1, authResp); err != nil {
					shadowConn.Close()
					return nil, fmt.Errorf("sending shadow auth switch response: %w", err)
				}
				continue
			}
			shadowConn.Close()
			return nil, fmt.Errorf("unexpected auth switch from shadow")
		case 0x01:
			// Auth more data. For caching_sha2_password, send full auth request.
			if len(result.Payload) > 1 && result.Payload[1] == 0x04 {
				// Full authentication required. Send password in cleartext.
				authResp := append([]byte("mori"), 0)
				if err := writeMySQLPacket(shadowConn, result.Sequence+1, authResp); err != nil {
					shadowConn.Close()
					return nil, fmt.Errorf("sending shadow full auth: %w", err)
				}
				continue
			}
			continue
		default:
			continue
		}
	}
}

// parseHandshake extracts auth data and plugin name from MySQL handshake.
func parseHandshake(payload []byte) (authData []byte, plugin string) {
	if len(payload) < 5 {
		return nil, ""
	}

	pos := 1 // Skip protocol version.

	// Skip server version (null-terminated).
	for pos < len(payload) && payload[pos] != 0 {
		pos++
	}
	pos++ // Skip null.

	if pos+4 > len(payload) {
		return nil, ""
	}
	pos += 4 // Skip connection ID (4 bytes).

	// Auth data part 1 (8 bytes).
	if pos+8 > len(payload) {
		return nil, ""
	}
	authData = make([]byte, 8)
	copy(authData, payload[pos:pos+8])
	pos += 8

	pos++ // Skip filler.

	if pos+2 > len(payload) {
		return authData, ""
	}
	pos += 2 // Skip capability flags lower.

	if pos+1 > len(payload) {
		return authData, ""
	}
	pos++ // Skip charset.

	if pos+2 > len(payload) {
		return authData, ""
	}
	pos += 2 // Skip status flags.

	if pos+2 > len(payload) {
		return authData, ""
	}
	pos += 2 // Skip capability flags upper.

	if pos+1 > len(payload) {
		return authData, ""
	}
	authDataLen := int(payload[pos])
	pos++

	pos += 10 // Skip reserved.

	// Auth data part 2 (max(13, authDataLen - 8) bytes).
	part2Len := authDataLen - 8
	if part2Len < 13 {
		part2Len = 13
	}
	if pos+part2Len > len(payload) {
		part2Len = len(payload) - pos
	}
	if part2Len > 0 {
		part2 := payload[pos : pos+part2Len]
		// Remove trailing null byte.
		if len(part2) > 0 && part2[len(part2)-1] == 0 {
			part2 = part2[:len(part2)-1]
		}
		authData = append(authData, part2...)
		pos += part2Len
	}

	// Auth plugin name (null-terminated).
	if pos < len(payload) {
		pluginEnd := pos
		for pluginEnd < len(payload) && payload[pluginEnd] != 0 {
			pluginEnd++
		}
		plugin = string(payload[pos:pluginEnd])
	}

	return authData, plugin
}

