package proxy

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

// buildHandshakeResponse constructs a MySQL HandshakeResponse41 packet payload.
func buildHandshakeResponse(user, password, dbName string, authData []byte, authPlugin string) []byte {
	var buf []byte

	// Capability flags (4 bytes LE).
	// CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH
	caps := uint32(0x00000200 | 0x00008000 | 0x00000008 | 0x00080000 | 0x00000001 | 0x00000004)
	var capBuf [4]byte
	binary.LittleEndian.PutUint32(capBuf[:], caps)
	buf = append(buf, capBuf[:]...)

	// Max packet size (4 bytes LE).
	var maxPacket [4]byte
	binary.LittleEndian.PutUint32(maxPacket[:], 16*1024*1024)
	buf = append(buf, maxPacket[:]...)

	// Character set (1 byte) — utf8mb4 = 45.
	buf = append(buf, 45)

	// Reserved (23 bytes of zeros).
	buf = append(buf, make([]byte, 23)...)

	// Username (null-terminated).
	buf = append(buf, []byte(user)...)
	buf = append(buf, 0)

	// Auth response.
	authResp := scramblePassword(authData, password)
	buf = append(buf, byte(len(authResp)))
	buf = append(buf, authResp...)

	// Database name (null-terminated).
	buf = append(buf, []byte(dbName)...)
	buf = append(buf, 0)

	// Auth plugin name (null-terminated).
	if authPlugin == "" {
		authPlugin = "mysql_native_password"
	}
	buf = append(buf, []byte(authPlugin)...)
	buf = append(buf, 0)

	return buf
}

// scramblePassword computes the MySQL native password auth response.
// SHA1(password) XOR SHA1(authData + SHA1(SHA1(password)))
func scramblePassword(authData []byte, password string) []byte {
	if password == "" {
		return nil
	}

	// SHA1(password)
	hash1 := sha1.Sum([]byte(password))

	// SHA1(SHA1(password))
	hash2 := sha1.Sum(hash1[:])

	// SHA1(authData + SHA1(SHA1(password)))
	h := sha1.New()
	h.Write(authData)
	h.Write(hash2[:])
	hash3 := h.Sum(nil)

	// XOR SHA1(password) with SHA1(authData + SHA1(SHA1(password)))
	result := make([]byte, sha1.Size)
	for i := range result {
		result[i] = hash1[i] ^ hash3[i]
	}
	return result
}

// buildCOMQuery constructs a COM_QUERY packet.
func buildCOMQuery(seq byte, sql string) []byte {
	payload := append([]byte{comQuery}, []byte(sql)...)
	return buildMySQLPacket(seq, payload)
}

// buildCOMPing constructs a COM_PING packet.
func buildCOMPing(seq byte) []byte {
	return buildMySQLPacket(seq, []byte{comPing})
}

// buildGuardErrorResponse constructs a MySQL ERR packet for write guard errors.
func buildGuardErrorResponse(seq byte, message string) []byte {
	return buildERRPacket(seq, 1105, "MR001", "mori: "+message)
}

// forwardAndRelay sends a packet to the backend and relays the complete response
// back to the client.
func forwardAndRelay(raw []byte, backend, client net.Conn) error {
	if _, err := backend.Write(raw); err != nil {
		return fmt.Errorf("sending to backend: %w", err)
	}

	return relayResponse(backend, client)
}

// relayResponse reads a complete MySQL response from backend and writes to client.
// A complete response is: OK, ERR, or result set (column defs + EOF + rows + EOF).
func relayResponse(backend, client net.Conn) error {
	// Read first packet.
	pkt, err := readMySQLPacket(backend)
	if err != nil {
		return fmt.Errorf("reading backend response: %w", err)
	}

	if _, err := client.Write(pkt.Raw); err != nil {
		return fmt.Errorf("relaying to client: %w", err)
	}

	// OK or ERR: response is complete.
	if isOKPacket(pkt.Payload) || isERRPacket(pkt.Payload) {
		return nil
	}

	// Result set: relay column definitions until EOF, then rows until EOF.
	// First, relay column definitions.
	for {
		pkt, err = readMySQLPacket(backend)
		if err != nil {
			return fmt.Errorf("reading column def: %w", err)
		}
		if _, err := client.Write(pkt.Raw); err != nil {
			return fmt.Errorf("relaying column def: %w", err)
		}
		if isEOFPacket(pkt.Payload) {
			break
		}
	}

	// Then relay rows until EOF or ERR.
	for {
		pkt, err = readMySQLPacket(backend)
		if err != nil {
			return fmt.Errorf("reading row data: %w", err)
		}
		if _, err := client.Write(pkt.Raw); err != nil {
			return fmt.Errorf("relaying row data: %w", err)
		}
		if isEOFPacket(pkt.Payload) || isERRPacket(pkt.Payload) {
			return nil
		}
	}
}

// drainResponse reads and discards a complete MySQL response from a connection.
func drainResponse(conn net.Conn) error {
	pkt, err := readMySQLPacket(conn)
	if err != nil {
		return err
	}
	if isOKPacket(pkt.Payload) || isERRPacket(pkt.Payload) {
		return nil
	}

	// Result set: drain column defs until EOF, then rows until EOF.
	for {
		pkt, err = readMySQLPacket(conn)
		if err != nil {
			return err
		}
		if isEOFPacket(pkt.Payload) {
			break
		}
	}
	for {
		pkt, err = readMySQLPacket(conn)
		if err != nil {
			return err
		}
		if isEOFPacket(pkt.Payload) || isERRPacket(pkt.Payload) {
			return nil
		}
	}
}

// findSemicolonOutsideQuotes returns the index of the first semicolon
// that is not inside a single-quoted string literal or a comment, or -1
// if none found.
func findSemicolonOutsideQuotes(s string) int {
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++ // skip escaped ''
				} else {
					inQuote = false
				}
			}
			continue
		}
		switch c {
		case '\'':
			inQuote = true
		case '-':
			// Line comment: skip to newline
			if i+1 < len(s) && s[i+1] == '-' {
				nl := strings.Index(s[i:], "\n")
				if nl >= 0 {
					i += nl // loop will increment past \n
				} else {
					return -1 // rest is a comment
				}
			}
		case '/':
			// Block comment: skip to */
			if i+1 < len(s) && s[i+1] == '*' {
				end := strings.Index(s[i+2:], "*/")
				if end >= 0 {
					i += 2 + end + 1 // skip past */
				} else {
					return -1 // unclosed block comment
				}
			}
		case ';':
			return i
		}
	}
	return -1
}

// stripLeadingComments removes leading SQL line comments (-- ...\n)
// and block comments (/* ... */) from a string.
func stripLeadingComments(s string) string {
	for {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "--") {
			if nl := strings.Index(s, "\n"); nl >= 0 {
				s = s[nl+1:]
			} else {
				return ""
			}
		} else if strings.HasPrefix(s, "/*") {
			if end := strings.Index(s, "*/"); end >= 0 {
				s = s[end+2:]
			} else {
				return ""
			}
		} else {
			return s
		}
	}
}

// looksLikeWrite returns true if the SQL appears to be a mutating statement.
func looksLikeWrite(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	writePrefixes := []string{
		"INSERT", "UPDATE", "DELETE", "REPLACE", "TRUNCATE",
		"CREATE", "ALTER", "DROP",
		"GRANT", "REVOKE", "RENAME",
		"LOAD DATA", "LOAD XML",
		"CALL",
	}

	safePrefixes := []string{
		"SELECT", "SET", "SHOW", "EXPLAIN", "DESCRIBE", "DESC",
		"BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION",
		"SAVEPOINT", "RELEASE", "DEALLOCATE", "USE", "HELP",
	}
	for _, prefix := range safePrefixes {
		if strings.HasPrefix(upper, prefix) {
			// Multi-statement: even if first statement is safe, check
			// subsequent statements after semicolons.
			if idx := findSemicolonOutsideQuotes(upper); idx >= 0 {
				if multiStmtHasWrite(upper[idx+1:], writePrefixes) {
					return true
				}
			}
			return false
		}
	}

	for _, prefix := range writePrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}

	// CTE writes.
	if strings.HasPrefix(upper, "WITH") {
		if containsKeyword(upper, "INSERT") ||
			containsKeyword(upper, "UPDATE") ||
			containsKeyword(upper, "DELETE") {
			return true
		}
	}

	return false
}

// multiStmtHasWrite checks whether any segment in the semicolon-separated
// remainder contains a write prefix or a CTE write.
func multiStmtHasWrite(rest string, writePrefixes []string) bool {
	for rest != "" {
		idx := findSemicolonOutsideQuotes(rest)
		var seg string
		if idx >= 0 {
			seg = rest[:idx]
			rest = rest[idx+1:]
		} else {
			seg = rest
			rest = ""
		}
		seg = strings.ToUpper(stripLeadingComments(strings.TrimSpace(seg)))
		if seg == "" {
			continue
		}
		for _, wp := range writePrefixes {
			if strings.HasPrefix(seg, wp) {
				return true
			}
		}
		if strings.HasPrefix(seg, "WITH") {
			if containsKeyword(seg, "INSERT") ||
				containsKeyword(seg, "UPDATE") ||
				containsKeyword(seg, "DELETE") {
				return true
			}
		}
	}
	return false
}

// containsKeyword checks if s contains keyword as a standalone word
// (not as a substring of an identifier like "insert_log").
func containsKeyword(s, keyword string) bool {
	for idx := 0; idx < len(s); {
		pos := strings.Index(s[idx:], keyword)
		if pos < 0 {
			return false
		}
		pos += idx
		leftOK := pos == 0 || !isIdentChar(s[pos-1])
		end := pos + len(keyword)
		rightOK := end >= len(s) || !isIdentChar(s[end])
		if leftOK && rightOK {
			return true
		}
		idx = pos + len(keyword)
	}
	return false
}

func truncateSQL(sql string, maxLen int) string {
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}
