package proxy

import (
	"fmt"
	"io"
	"net"
	"time"
)

// relayHandshake relays the MySQL handshake between client and production server.
// MySQL handshake flow:
// 1. Server -> Client: Initial Handshake Packet
// 2. Client -> Server: Handshake Response Packet
// 3. Server -> Client: OK/ERR/AuthSwitch
// 4. If AuthSwitch: Client -> Server: auth data, Server -> Client: OK/ERR
func relayHandshake(clientConn, prodConn net.Conn) error {
	// 1. Read initial handshake from Prod.
	handshake, err := readMySQLPacket(prodConn)
	if err != nil {
		return fmt.Errorf("reading prod handshake: %w", err)
	}

	// Forward to client.
	if _, err := clientConn.Write(handshake.Raw); err != nil {
		return fmt.Errorf("forwarding handshake to client: %w", err)
	}

	// 2. Read handshake response from client.
	response, err := readMySQLPacket(clientConn)
	if err != nil {
		return fmt.Errorf("reading client handshake response: %w", err)
	}

	// Forward to Prod.
	if _, err := prodConn.Write(response.Raw); err != nil {
		return fmt.Errorf("forwarding handshake response to prod: %w", err)
	}

	// 3. Read auth result from Prod.
	// May need multiple rounds for auth switch.
	for {
		authResult, err := readMySQLPacket(prodConn)
		if err != nil {
			return fmt.Errorf("reading prod auth result: %w", err)
		}

		// Forward to client.
		if _, err := clientConn.Write(authResult.Raw); err != nil {
			return fmt.Errorf("forwarding auth result to client: %w", err)
		}

		if len(authResult.Payload) == 0 {
			return fmt.Errorf("empty auth result from prod")
		}

		switch authResult.Payload[0] {
		case iOK:
			return nil
		case iERR:
			return fmt.Errorf("prod auth error: %s", string(authResult.Payload[9:]))
		case iEOF:
			// AuthSwitch or old-auth. Relay client response.
			if len(authResult.Payload) == 1 {
				// Old-auth: client sends password.
				clientAuth, err := readMySQLPacket(clientConn)
				if err != nil {
					return fmt.Errorf("reading client old-auth: %w", err)
				}
				if _, err := prodConn.Write(clientAuth.Raw); err != nil {
					return fmt.Errorf("forwarding old-auth to prod: %w", err)
				}
				continue
			}
			return nil
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
					return fmt.Errorf("reading client auth continuation: %w", err)
				}
				if _, err := prodConn.Write(clientAuth.Raw); err != nil {
					return fmt.Errorf("forwarding auth continuation to prod: %w", err)
				}
				continue
			}
			if authResult.Payload[0] == 0xfe {
				// Auth switch request (0xFE with plugin name).
				clientAuth, err := readMySQLPacket(clientConn)
				if err != nil {
					return fmt.Errorf("reading client auth switch response: %w", err)
				}
				if _, err := prodConn.Write(clientAuth.Raw); err != nil {
					return fmt.Errorf("forwarding auth switch response to prod: %w", err)
				}
				continue
			}
			return nil
		}
	}
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

// readRawBytes reads exactly n bytes from the reader.
func readRawBytes(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
