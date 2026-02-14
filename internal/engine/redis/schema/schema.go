package schema

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/mori-dev/mori/internal/engine/redis/connstr"
)

// DetectKeyMetadata connects to a Redis instance and discovers key patterns.
// Uses SCAN to iterate keys and TYPE to classify them by prefix.
func DetectKeyMetadata(info *connstr.ConnInfo) (map[string]KeyMeta, error) {
	conn, err := net.DialTimeout("tcp", info.Addr(), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)

	// AUTH if needed.
	if info.Password != "" {
		if err := sendCommand(conn, r, "AUTH", info.Password); err != nil {
			return nil, fmt.Errorf("AUTH: %w", err)
		}
	}

	// SELECT db if not default.
	if info.DB != 0 {
		if err := sendCommand(conn, r, "SELECT", fmt.Sprintf("%d", info.DB)); err != nil {
			return nil, fmt.Errorf("SELECT: %w", err)
		}
	}

	// SCAN through all keys.
	prefixCounts := make(map[string]int)
	prefixTypes := make(map[string]string)
	cursor := "0"

	for {
		// Send SCAN command.
		resp, err := sendCommandGetReply(conn, r, "SCAN", cursor, "COUNT", "500")
		if err != nil {
			return nil, fmt.Errorf("SCAN: %w", err)
		}

		// Parse SCAN response: [cursor, [key1, key2, ...]]
		if len(resp) < 2 {
			break
		}
		cursor = resp[0]
		keys := parseScanKeys(resp[1:])

		for _, key := range keys {
			prefix := keyPrefix(key)
			prefixCounts[prefix]++

			// Get TYPE for first key of each prefix.
			if _, exists := prefixTypes[prefix]; !exists {
				typeResp, err := sendCommandGetSingle(conn, r, "TYPE", key)
				if err == nil {
					prefixTypes[prefix] = typeResp
				}
			}
		}

		if cursor == "0" {
			break
		}
	}

	// Build result.
	tables := make(map[string]KeyMeta, len(prefixCounts))
	for prefix, count := range prefixCounts {
		keyType := prefixTypes[prefix]
		if keyType == "" {
			keyType = "string"
		}
		tables[prefix] = KeyMeta{
			Prefix:    prefix,
			Type:      keyType,
			Count:     count,
			PKColumns: []string{"key"},
			PKType:    "uuid",
		}
	}

	return tables, nil
}

// keyPrefix extracts the prefix from a Redis key (before first ':').
func keyPrefix(key string) string {
	if idx := strings.Index(key, ":"); idx >= 0 {
		return key[:idx]
	}
	return key
}

// sendCommand sends a Redis command and reads an OK-ish response.
func sendCommand(conn net.Conn, r *bufio.Reader, args ...string) error {
	cmd := formatCommand(args...)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "-") {
		return fmt.Errorf("redis error: %s", line[1:])
	}
	return nil
}

// sendCommandGetSingle sends a command and returns the single-line response.
func sendCommandGetSingle(conn net.Conn, r *bufio.Reader, args ...string) (string, error) {
	cmd := formatCommand(args...)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return "", err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "-") {
		return "", fmt.Errorf("redis error: %s", line[1:])
	}
	if strings.HasPrefix(line, "+") {
		return line[1:], nil
	}
	return line, nil
}

// sendCommandGetReply sends a command and parses a multi-line SCAN-style response.
func sendCommandGetReply(conn net.Conn, r *bufio.Reader, args ...string) ([]string, error) {
	cmd := formatCommand(args...)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return nil, err
	}
	return readArrayResponse(r)
}

// readArrayResponse reads a RESP array response and returns flattened string elements.
func readArrayResponse(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if strings.HasPrefix(line, "-") {
		return nil, fmt.Errorf("redis error: %s", line[1:])
	}

	if strings.HasPrefix(line, "*") {
		count := 0
		fmt.Sscanf(line, "*%d", &count)
		if count < 0 {
			return nil, nil
		}
		var result []string
		for i := 0; i < count; i++ {
			elem, err := readElement(r)
			if err != nil {
				return nil, err
			}
			result = append(result, elem...)
		}
		return result, nil
	}

	if strings.HasPrefix(line, "$") {
		length := 0
		fmt.Sscanf(line, "$%d", &length)
		if length < 0 {
			return []string{""}, nil
		}
		buf := make([]byte, length+2)
		if _, err := r.Read(buf); err != nil {
			return nil, err
		}
		return []string{string(buf[:length])}, nil
	}

	if strings.HasPrefix(line, "+") {
		return []string{line[1:]}, nil
	}

	if strings.HasPrefix(line, ":") {
		return []string{line[1:]}, nil
	}

	return []string{line}, nil
}

// readElement reads a single RESP element (could be bulk string, array, etc.).
func readElement(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if strings.HasPrefix(line, "$") {
		length := 0
		fmt.Sscanf(line, "$%d", &length)
		if length < 0 {
			return []string{""}, nil
		}
		buf := make([]byte, length+2)
		if _, err := r.Read(buf); err != nil {
			return nil, err
		}
		return []string{string(buf[:length])}, nil
	}

	if strings.HasPrefix(line, "*") {
		count := 0
		fmt.Sscanf(line, "*%d", &count)
		if count < 0 {
			return nil, nil
		}
		var result []string
		for i := 0; i < count; i++ {
			elem, err := readElement(r)
			if err != nil {
				return nil, err
			}
			result = append(result, elem...)
		}
		return result, nil
	}

	if strings.HasPrefix(line, "+") {
		return []string{line[1:]}, nil
	}
	if strings.HasPrefix(line, ":") {
		return []string{line[1:]}, nil
	}

	return []string{line}, nil
}

// parseScanKeys extracts keys from the flattened SCAN response.
func parseScanKeys(parts []string) []string {
	return parts
}

// formatCommand formats a Redis inline command into RESP protocol.
func formatCommand(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return b.String()
}

// GetRedisInfo connects to Redis and returns INFO server output.
func GetRedisInfo(info *connstr.ConnInfo) (string, error) {
	conn, err := net.DialTimeout("tcp", info.Addr(), 10*time.Second)
	if err != nil {
		return "", fmt.Errorf("connect to Redis: %w", err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)

	// AUTH if needed.
	if info.Password != "" {
		if err := sendCommand(conn, r, "AUTH", info.Password); err != nil {
			return "", fmt.Errorf("AUTH: %w", err)
		}
	}

	// Send INFO server.
	cmd := formatCommand("INFO", "server")
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return "", err
	}

	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "-") {
		return "", fmt.Errorf("redis error: %s", line[1:])
	}

	// Bulk string response.
	if strings.HasPrefix(line, "$") {
		length := 0
		fmt.Sscanf(line, "$%d", &length)
		if length < 0 {
			return "", nil
		}
		buf := make([]byte, length+2)
		if _, err := r.Read(buf); err != nil {
			return "", err
		}
		return string(buf[:length]), nil
	}

	return line, nil
}

// ParseRedisVersion extracts the Redis version from INFO server output.
func ParseRedisVersion(infoOutput string) string {
	for _, line := range strings.Split(infoOutput, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimPrefix(line, "redis_version:")
		}
	}
	return "unknown"
}

// PingRedis checks connectivity to a Redis instance.
func PingRedis(info *connstr.ConnInfo) error {
	conn, err := net.DialTimeout("tcp", info.Addr(), 10*time.Second)
	if err != nil {
		return fmt.Errorf("connect to Redis: %w", err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)

	// AUTH if needed.
	if info.Password != "" {
		if err := sendCommand(conn, r, "AUTH", info.Password); err != nil {
			return fmt.Errorf("AUTH: %w", err)
		}
	}

	// PING.
	cmd := formatCommand("PING")
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	if line != "+PONG" {
		return fmt.Errorf("unexpected PING response: %s", line)
	}
	return nil
}
