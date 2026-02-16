package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// firestoreHandler implements MCP tools for Firestore engines.
// Connects to the Mori proxy (gRPC) which routes to prod/shadow.
type firestoreHandler struct {
	proxyAddr string
	projectID string
}

func newFirestoreHandler(cfg EngineConfig) *firestoreHandler {
	return &firestoreHandler{
		proxyAddr: fmt.Sprintf("127.0.0.1:%d", cfg.ProxyPort),
		projectID: cfg.DBName,
	}
}

// registerFirestoreTools registers Firestore-specific MCP tools.
func (s *Server) registerFirestoreTools(mcpSrv *server.MCPServer) {
	h := newFirestoreHandler(s.cfg)

	mcpSrv.AddTool(
		mcplib.NewTool("firestore_get",
			mcplib.WithDescription("Get a Firestore document by collection and document ID"),
			mcplib.WithString("collection",
				mcplib.Description("Collection name (e.g. 'users', 'orders')"),
				mcplib.Required(),
			),
			mcplib.WithString("document_id",
				mcplib.Description("Document ID"),
				mcplib.Required(),
			),
		),
		h.handleGet,
	)

	mcpSrv.AddTool(
		mcplib.NewTool("firestore_list",
			mcplib.WithDescription("List documents in a Firestore collection"),
			mcplib.WithString("collection",
				mcplib.Description("Collection name"),
				mcplib.Required(),
			),
			mcplib.WithNumber("limit",
				mcplib.Description("Maximum number of documents to return (default: 25, max: 100)"),
			),
		),
		h.handleList,
	)

	mcpSrv.AddTool(
		mcplib.NewTool("firestore_query",
			mcplib.WithDescription("Query a Firestore collection with a field filter"),
			mcplib.WithString("collection",
				mcplib.Description("Collection name"),
				mcplib.Required(),
			),
			mcplib.WithString("field",
				mcplib.Description("Field name to filter on"),
				mcplib.Required(),
			),
			mcplib.WithString("op",
				mcplib.Description("Comparison operator: ==, !=, <, <=, >, >=, in, array-contains"),
				mcplib.Required(),
			),
			mcplib.WithString("value",
				mcplib.Description("Value to compare against (as string; numbers are auto-detected)"),
				mcplib.Required(),
			),
			mcplib.WithNumber("limit",
				mcplib.Description("Maximum number of documents to return (default: 25, max: 100)"),
			),
		),
		h.handleQuery,
	)
}

func (h *firestoreHandler) newClient(ctx context.Context) (*firestore.Client, error) {
	// Connect to the Mori proxy as if it were the Firestore emulator.
	client, err := firestore.NewClient(ctx, h.projectID,
		option.WithEndpoint(h.proxyAddr),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpcInsecureDialOption()),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to Mori proxy: %w", err)
	}
	return client, nil
}

func (h *firestoreHandler) handleGet(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	collection, err := request.RequireString("collection")
	if err != nil {
		return mcpError("Error: 'collection' argument is required"), nil
	}
	docID, err := request.RequireString("document_id")
	if err != nil {
		return mcpError("Error: 'document_id' argument is required"), nil
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := h.newClient(ctx)
	if err != nil {
		return mcpError(fmt.Sprintf("Error: %v", err)), nil
	}
	defer client.Close()

	doc, err := client.Collection(collection).Doc(docID).Get(ctx)
	if err != nil {
		return mcpError(fmt.Sprintf("Firestore error: %v", err)), nil
	}

	result := map[string]any{
		"_id":   doc.Ref.ID,
		"_path": doc.Ref.Path,
		"data":  doc.Data(),
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return mcpError(fmt.Sprintf("Error marshaling result: %v", err)), nil
	}
	return mcpOK(string(data)), nil
}

func (h *firestoreHandler) handleList(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	collection, err := request.RequireString("collection")
	if err != nil {
		return mcpError("Error: 'collection' argument is required"), nil
	}

	limit := 25
	if n := getOptionalInt(request, "limit"); n > 0 {
		limit = n
	}
	if limit > 100 {
		limit = 100
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := h.newClient(ctx)
	if err != nil {
		return mcpError(fmt.Sprintf("Error: %v", err)), nil
	}
	defer client.Close()

	iter := client.Collection(collection).Limit(limit).Documents(ctx)
	var results []map[string]any
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return mcpError(fmt.Sprintf("Firestore error: %v", err)), nil
		}
		entry := map[string]any{
			"_id":  doc.Ref.ID,
			"data": doc.Data(),
		}
		results = append(results, entry)
	}

	if results == nil {
		results = []map[string]any{}
	}
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return mcpError(fmt.Sprintf("Error marshaling results: %v", err)), nil
	}
	text := fmt.Sprintf("%d document(s)\n\n%s", len(results), string(data))
	return mcpOK(text), nil
}

func (h *firestoreHandler) handleQuery(ctx context.Context, request mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	collection, err := request.RequireString("collection")
	if err != nil {
		return mcpError("Error: 'collection' argument is required"), nil
	}
	field, err := request.RequireString("field")
	if err != nil {
		return mcpError("Error: 'field' argument is required"), nil
	}
	op, err := request.RequireString("op")
	if err != nil {
		return mcpError("Error: 'op' argument is required"), nil
	}
	valueStr, err := request.RequireString("value")
	if err != nil {
		return mcpError("Error: 'value' argument is required"), nil
	}

	limit := 25
	if l, err := request.RequireString("limit"); err == nil && l != "" {
		if n := parseIntSafe(l); n > 0 {
			limit = n
		}
	}
	if limit > 100 {
		limit = 100
	}

	// Auto-detect value type.
	value := autoDetectValue(valueStr)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := h.newClient(ctx)
	if err != nil {
		return mcpError(fmt.Sprintf("Error: %v", err)), nil
	}
	defer client.Close()

	q := client.Collection(collection).Where(field, op, value).Limit(limit)
	iter := q.Documents(ctx)
	var results []map[string]any
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return mcpError(fmt.Sprintf("Firestore error: %v", err)), nil
		}
		entry := map[string]any{
			"_id":  doc.Ref.ID,
			"data": doc.Data(),
		}
		results = append(results, entry)
	}

	if results == nil {
		results = []map[string]any{}
	}
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return mcpError(fmt.Sprintf("Error marshaling results: %v", err)), nil
	}
	text := fmt.Sprintf("%d document(s)\n\n%s", len(results), string(data))
	return mcpOK(text), nil
}

// grpcInsecureDialOption returns a gRPC dial option for insecure (plaintext) connections.
func grpcInsecureDialOption() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// autoDetectValue tries to parse a string as a number or boolean,
// falling back to string.
func autoDetectValue(s string) any {
	s = strings.TrimSpace(s)
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}
	if n := parseIntSafe(s); n != 0 || s == "0" {
		return n
	}
	if f := parseFloatSafe(s); f != 0 || s == "0.0" || s == "0.00" {
		return f
	}
	return s
}

func parseIntSafe(s string) int {
	n := 0
	negative := false
	for i, c := range s {
		if i == 0 && c == '-' {
			negative = true
			continue
		}
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	if negative {
		return -n
	}
	return n
}

func parseFloatSafe(s string) float64 {
	var n float64
	var frac float64
	fracDiv := 1.0
	inFrac := false
	negative := false
	for i, c := range s {
		if i == 0 && c == '-' {
			negative = true
			continue
		}
		if c == '.' {
			if inFrac {
				return 0
			}
			inFrac = true
			continue
		}
		if c < '0' || c > '9' {
			return 0
		}
		if inFrac {
			fracDiv *= 10
			frac += float64(c-'0') / fracDiv
		} else {
			n = n*10 + float64(c-'0')
		}
	}
	result := n + frac
	if negative {
		return -result
	}
	return result
}
