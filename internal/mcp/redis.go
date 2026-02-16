package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// redisHandler implements MCP tools for Redis engines.
// Connects to the Mori proxy using the RESP protocol.
type redisHandler struct {
	addr     string
	password string
}

func newRedisHandler(cfg EngineConfig) *redisHandler {
	return &redisHandler{
		addr:     fmt.Sprintf("127.0.0.1:%d", cfg.ProxyPort),
		password: cfg.Password,
	}
}

// registerRedisTools registers Redis-specific MCP tools.
func (s *Server) registerRedisTools(mcpSrv *server.MCPServer) {
	h := newRedisHandler(s.cfg)

	mcpSrv.AddTool(
		mcplib.NewTool("redis_command",
			mcplib.WithDescription("Execute a Redis command through Mori (e.g. 'SET key value', 'GET key')"),
			mcplib.WithString("command",
				mcplib.Description("Full Redis command to execute (e.g. 'SET mykey hello', 'HGETALL myhash')"),
				mcplib.Required(),
			),
		),
		h.handleCommand,
	)

	mcpSrv.AddTool(
		mcplib.NewTool("redis_get",
			mcplib.WithDescription("Get the value of a Redis key"),
			mcplib.WithString("key",
				mcplib.Description("Redis key to retrieve"),
				mcplib.Required(),
			),
		),
		h.handleGet,
	)

	mcpSrv.AddTool(
		mcplib.NewTool("redis_hgetall",
			mcplib.WithDescription("Get all fields and values of a Redis hash"),
			mcplib.WithString("key",
				mcplib.Description("Redis hash key"),
				mcplib.Required(),
			),
		),
		h.handleHGetAll,
	)

	mcpSrv.AddTool(
		mcplib.NewTool("redis_keys",
			mcplib.WithDescription("Find keys matching a pattern"),
			mcplib.WithString("pattern",
				mcplib.Description("Key pattern (e.g. 'user:*', 'session:*')"),
				mcplib.Required(),
			),
		),
		h.handleKeys,
	)
}

func (h *redisHandler) handleCommand(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	cmd, err := request.RequireString("command")
	if err != nil {
		return mcpError("Error: 'command' argument is required"), nil
	}
	cmd = strings.TrimSpace(cmd)
	if cmd == "" {
		return mcpError("Error: command cannot be empty"), nil
	}

	args := parseRedisCommand(cmd)
	result, err := h.execRedis(ctx, args)
	if err != nil {
		return mcpError(fmt.Sprintf("Redis error: %v", err)), nil
	}
	return mcpOK(result), nil
}

func (h *redisHandler) handleGet(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	key, err := request.RequireString("key")
	if err != nil {
		return mcpError("Error: 'key' argument is required"), nil
	}

	result, err := h.execRedis(ctx, []string{"GET", key})
	if err != nil {
		return mcpError(fmt.Sprintf("Redis error: %v", err)), nil
	}
	return mcpOK(result), nil
}

func (h *redisHandler) handleHGetAll(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	key, err := request.RequireString("key")
	if err != nil {
		return mcpError("Error: 'key' argument is required"), nil
	}

	result, err := h.execRedis(ctx, []string{"HGETALL", key})
	if err != nil {
		return mcpError(fmt.Sprintf("Redis error: %v", err)), nil
	}
	return mcpOK(result), nil
}

func (h *redisHandler) handleKeys(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	pattern, err := request.RequireString("pattern")
	if err != nil {
		return mcpError("Error: 'pattern' argument is required"), nil
	}

	result, err := h.execRedis(ctx, []string{"KEYS", pattern})
	if err != nil {
		return mcpError(fmt.Sprintf("Redis error: %v", err)), nil
	}
	return mcpOK(result), nil
}

// execRedis sends a RESP command and returns the formatted response.
func (h *redisHandler) execRedis(ctx context.Context, args []string) (string, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(10 * time.Second)
	}

	conn, err := net.DialTimeout("tcp", h.addr, 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()
	conn.SetDeadline(deadline)

	reader := bufio.NewReader(conn)

	// Authenticate if password is set.
	if h.password != "" {
		if _, err := conn.Write(encodeRESP([]string{"AUTH", h.password})); err != nil {
			return "", fmt.Errorf("auth write: %w", err)
		}
		if _, err := readRESPValue(reader); err != nil {
			return "", fmt.Errorf("auth: %w", err)
		}
	}

	// Send command.
	if _, err := conn.Write(encodeRESP(args)); err != nil {
		return "", fmt.Errorf("write: %w", err)
	}

	// Read response.
	val, err := readRESPValue(reader)
	if err != nil {
		return "", err
	}

	return formatRESPValue(val), nil
}

// encodeRESP encodes a command as a RESP array.
func encodeRESP(args []string) []byte {
	var buf []byte
	buf = append(buf, fmt.Sprintf("*%d\r\n", len(args))...)
	for _, arg := range args {
		buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)...)
	}
	return buf
}

// readRESPValue reads a single RESP value from the reader.
func readRESPValue(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	switch line[0] {
	case '+': // Simple string
		return line[1:], nil
	case '-': // Error
		return nil, fmt.Errorf("%s", line[1:])
	case ':': // Integer
		n, _ := strconv.ParseInt(line[1:], 10, 64)
		return n, nil
	case '$': // Bulk string
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil // null bulk string
		}
		buf := make([]byte, n+2) // +2 for \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read bulk: %w", err)
		}
		return string(buf[:n]), nil
	case '*': // Array
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil // null array
		}
		arr := make([]any, n)
		for i := 0; i < n; i++ {
			v, err := readRESPValue(r)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	default:
		return line, nil
	}
}

// formatRESPValue formats a RESP value for display.
func formatRESPValue(val any) string {
	switch v := val.(type) {
	case nil:
		return "(nil)"
	case string:
		return v
	case int64:
		return fmt.Sprintf("(integer) %d", v)
	case []any:
		if len(v) == 0 {
			return "(empty list)"
		}
		// Try to detect hash-like response (even-length array of alternating key-value).
		if len(v)%2 == 0 && len(v) >= 2 {
			isHash := true
			for i := 0; i < len(v); i += 2 {
				if _, ok := v[i].(string); !ok {
					isHash = false
					break
				}
			}
			if isHash {
				m := make(map[string]any, len(v)/2)
				for i := 0; i < len(v); i += 2 {
					key := v[i].(string)
					m[key] = v[i+1]
				}
				data, err := json.MarshalIndent(m, "", "  ")
				if err == nil {
					return string(data)
				}
			}
		}
		// Format as numbered list.
		var lines []string
		for i, item := range v {
			lines = append(lines, fmt.Sprintf("%d) %s", i+1, formatRESPValue(item)))
		}
		return strings.Join(lines, "\n")
	default:
		return fmt.Sprintf("%v", v)
	}
}

// parseRedisCommand splits a command string into arguments, respecting quotes.
func parseRedisCommand(cmd string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		if inQuote {
			if c == quoteChar {
				inQuote = false
			} else {
				current.WriteByte(c)
			}
		} else if c == '\'' || c == '"' {
			inQuote = true
			quoteChar = c
		} else if c == ' ' || c == '\t' {
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		} else {
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args
}
