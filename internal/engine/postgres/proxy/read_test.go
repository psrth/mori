package proxy

import (
	"net"
	"sync"
	"testing"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
	"github.com/psrth/mori/internal/engine/postgres/schema"
)

// newTestReadHandler creates a ReadHandler with mock connections and fresh state.
func newTestReadHandler(
	prodConn, shadowConn net.Conn,
	tables map[string]schema.TableMeta,
	t *testing.T,
) *ReadHandler {
	return &ReadHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   delta.NewMap(),
		tombstones: delta.NewTombstoneSet(),
		tables:     tables,
		connID:     1,
		verbose:    false,
	}
}

// parseClientResponse parses the raw PG response received by the client.
// Returns the number of data rows and the column names.
func parseClientResponse(raw []byte) (rowCount int, columns []string, err error) {
	pos := 0
	for pos < len(raw) {
		if pos+5 > len(raw) {
			break
		}
		msgType := raw[pos]
		msgLen := int(raw[pos+1])<<24 | int(raw[pos+2])<<16 | int(raw[pos+3])<<8 | int(raw[pos+4])
		totalLen := 1 + msgLen
		if pos+totalLen > len(raw) {
			break
		}
		payload := raw[pos+5 : pos+totalLen]

		switch msgType {
		case 'T': // RowDescription
			cols, e := parseRowDescription(payload)
			if e != nil {
				return 0, nil, e
			}
			for _, c := range cols {
				columns = append(columns, c.Name)
			}
		case 'D': // DataRow
			rowCount++
		}

		pos += totalLen
	}
	return rowCount, columns, nil
}

// --- Phase 8: Single-Table Merged Read Tests ---

func TestMergedReadBasic(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)

	// Shadow returns 1 row (locally inserted).
	shadowBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"100", "local-user"}},
		"SELECT 1",
	))

	// Prod returns 3 rows.
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"1", "alice"}, {"2", "bob"}, {"3", "charlie"}},
		"SELECT 3",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name FROM users",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	rowCount, columns, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 4 {
		t.Errorf("expected 4 rows, got %d", rowCount)
	}
	if len(columns) < 2 || columns[0] != "id" || columns[1] != "name" {
		t.Errorf("unexpected columns: %v", columns)
	}
}

func TestMergedReadWithDelta(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)
	rh.deltaMap.Add("users", "42")

	// Shadow returns the delta row (updated version).
	shadowBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"42", "bob-updated"}},
		"SELECT 1",
	))

	// Prod returns both rows (including the stale version of 42).
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"42", "bob-original"}, {"43", "charlie"}},
		"SELECT 2",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name FROM users",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// Should have 2 rows: pk=42 from Shadow, pk=43 from Prod. Prod's pk=42 filtered out.
	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows (delta filtered), got %d", rowCount)
	}
}

func TestMergedReadWithTombstone(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)
	rh.tombstones.Add("users", "99")

	// Shadow returns 0 rows.
	shadowBackend.respondWith(emptySelectResponse([]string{"id", "name"}))

	// Prod returns 2 rows including the tombstoned one.
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"98", "alice"}, {"99", "deleted-user"}},
		"SELECT 2",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name FROM users",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// Should have 1 row: pk=98 only. pk=99 is tombstoned.
	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected 1 row (tombstone filtered), got %d", rowCount)
	}
}

func TestMergedReadWithOrderBy(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)

	// Shadow returns 1 row with id=100.
	shadowBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"100", "zebra"}},
		"SELECT 1",
	))

	// Prod returns 1 row with id=1.
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"1", "alice"}},
		"SELECT 1",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name FROM users ORDER BY id ASC",
		OrderBy: "id ASC",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows, got %d", rowCount)
	}
}

func TestMergedReadWithLimit(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)
	// Add a delta so overfetch triggers.
	rh.deltaMap.Add("users", "5")

	// Shadow returns 1 row.
	shadowBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"5", "shadow-five"}},
		"SELECT 1",
	))

	// Prod returns 4 rows (over-fetched from LIMIT 3 + 1 delta = 4).
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"1", "a"}, {"2", "b"}, {"3", "c"}, {"5", "prod-five"}},
		"SELECT 4",
	))

	cl := &core.Classification{
		OpType:   core.OpRead,
		SubType:  core.SubSelect,
		Tables:   []string{"users"},
		RawSQL:   "SELECT id, name FROM users LIMIT 3",
		HasLimit: true,
		Limit:    3,
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// Shadow: pk=5 (1 row), Prod after filter: pk=1,2,3 (3 rows, pk=5 filtered as delta).
	// Merged: 4 rows, LIMIT 3 applied -> 3 rows.
	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 3 {
		t.Errorf("expected 3 rows (LIMIT applied), got %d", rowCount)
	}
}

func TestMergedReadSchemaAdaptation(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)

	// Set up schema registry with an added column.
	reg := coreSchema.NewRegistry()
	reg.RecordAddColumn("users", coreSchema.Column{Name: "phone", Type: "TEXT"})
	rh.schemaRegistry = reg

	// Shadow returns 1 row WITH the phone column.
	shadowBackend.respondWith(selectResponse(
		[]string{"id", "name", "phone"},
		[][]interface{}{{"100", "new-user", "555-0100"}},
		"SELECT 1",
	))

	// Prod returns 1 row WITHOUT the phone column.
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"1", "alice"}},
		"SELECT 1",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name, phone FROM users",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// Should have 2 rows, and the result should include the phone column
	// (Shadow columns are canonical).
	rowCount, columns, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows, got %d", rowCount)
	}
	if len(columns) != 3 {
		t.Errorf("expected 3 columns (with phone), got %d: %v", len(columns), columns)
	}
}

func TestMergedReadEmptyShadow(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)
	// Need a delta/tombstone so this gets routed as merged read.
	rh.tombstones.Add("users", "999")

	// Shadow returns 0 rows.
	shadowBackend.respondWith(emptySelectResponse([]string{"id", "name"}))

	// Prod returns 2 rows.
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"1", "alice"}, {"2", "bob"}},
		"SELECT 2",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name FROM users",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows, got %d", rowCount)
	}
}

func TestMergedReadEmptyProd(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)

	// Shadow returns 2 rows.
	shadowBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"100", "local-a"}, {"101", "local-b"}},
		"SELECT 2",
	))

	// Prod returns 0 rows.
	prodBackend.respondWith(emptySelectResponse([]string{"id", "name"}))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name FROM users",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows (from Shadow only), got %d", rowCount)
	}
}

func TestMergedReadProdError(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)

	// Shadow returns normally.
	shadowBackend.respondWith(emptySelectResponse([]string{"id", "name"}))

	// Prod returns an error.
	prodBackend.respondWith(func() []byte {
		var buf []byte
		buf = append(buf, makeErrorResponseMsg("relation \"users\" does not exist")...)
		buf = append(buf, makeReadyForQueryMsg()...)
		return buf
	}())

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name FROM users",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// Client should have received something (the error response from Prod).
	if len(clientBuf) == 0 {
		t.Error("expected non-empty error response relayed to client")
	}
}

func TestMergedReadDedup(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)

	// Both Shadow and Prod return pk=1. Shadow's version should win.
	shadowBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"1", "shadow-version"}},
		"SELECT 1",
	))

	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"1", "prod-version"}, {"2", "unique"}},
		"SELECT 2",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT id, name FROM users",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyMergedRead)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// Should have 2 rows: pk=1 (from Shadow, deduped) and pk=2 (from Prod).
	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows (deduped), got %d", rowCount)
	}
}

func TestMergedReadUnsupportedStrategy(t *testing.T) {
	rh := &ReadHandler{deltaMap: delta.NewMap(), tombstones: delta.NewTombstoneSet()}
	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	err := rh.HandleRead(clientConn, nil, &core.Classification{}, core.StrategyProdDirect)
	if err == nil {
		t.Error("expected error for unsupported strategy")
	}
}

// --- Phase 9: JOIN Merged Read Tests ---

func TestJoinPatchBasic(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users":  {PKColumns: []string{"user_id"}, PKType: "serial"},
		"orders": {PKColumns: []string{"order_id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)
	rh.deltaMap.Add("users", "42")

	// Prod returns JOIN result with user 42 (stale) and user 43 (clean).
	prodBackend.respondWith(selectResponse(
		[]string{"order_id", "user_id", "name", "total"},
		[][]interface{}{
			{"1", "42", "old-name", "100"},
			{"2", "43", "charlie", "200"},
		},
		"SELECT 2",
	))

	// Shadow responds to the patch fetch: SELECT * FROM users WHERE user_id = 42
	// Then responds to the full JOIN query.
	shadowBackend.respondToMultiple([][]byte{
		// Patch fetch for user 42.
		selectResponse(
			[]string{"user_id", "name"},
			[][]interface{}{{"42", "new-name"}},
			"SELECT 1",
		),
		// Shadow JOIN result (locally inserted rows, if any).
		emptySelectResponse([]string{"order_id", "user_id", "name", "total"}),
	})

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"orders", "users"},
		IsJoin:  true,
		RawSQL:  "SELECT o.order_id, u.user_id, u.name, o.total FROM orders o JOIN users u ON o.user_id = u.user_id",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyJoinPatch)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// Should have 2 rows: both orders, with user 42's name patched.
	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows, got %d", rowCount)
	}
}

func TestJoinPatchWithTombstone(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users":  {PKColumns: []string{"user_id"}, PKType: "serial"},
		"orders": {PKColumns: []string{"order_id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)
	rh.tombstones.Add("users", "99")

	// Prod returns JOIN result including orders for tombstoned user 99.
	prodBackend.respondWith(selectResponse(
		[]string{"order_id", "user_id", "total"},
		[][]interface{}{
			{"1", "98", "100"},
			{"2", "99", "200"},
			{"3", "99", "300"},
		},
		"SELECT 3",
	))

	// Shadow JOIN result (no local rows).
	shadowBackend.respondWith(emptySelectResponse([]string{"order_id", "user_id", "total"}))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"orders", "users"},
		IsJoin:  true,
		RawSQL:  "SELECT o.order_id, u.user_id, o.total FROM orders o JOIN users u ON o.user_id = u.user_id",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyJoinPatch)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// User 99's orders should be discarded. Only order for user 98 remains.
	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected 1 row (tombstoned user's orders removed), got %d", rowCount)
	}
}

func TestJoinPatchLocalInsert(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users":  {PKColumns: []string{"user_id"}, PKType: "serial"},
		"orders": {PKColumns: []string{"order_id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)
	// Need a delta so this gets routed as join patch.
	rh.deltaMap.Add("users", "100001")

	// Prod returns 1 clean row.
	prodBackend.respondWith(selectResponse(
		[]string{"order_id", "user_id", "total"},
		[][]interface{}{{"1", "42", "100"}},
		"SELECT 1",
	))

	// Shadow JOIN returns the locally inserted user+order pair.
	shadowBackend.respondWith(selectResponse(
		[]string{"order_id", "user_id", "total"},
		[][]interface{}{{"100001", "100001", "500"}},
		"SELECT 1",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"orders", "users"},
		IsJoin:  true,
		RawSQL:  "SELECT o.order_id, u.user_id, o.total FROM orders o JOIN users u ON o.user_id = u.user_id",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyJoinPatch)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	// Should have 2 rows: 1 from Prod + 1 from Shadow.
	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected 2 rows (Prod + Shadow local insert), got %d", rowCount)
	}
}

func TestJoinPatchNoDeltaTables(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users":  {PKColumns: []string{"id"}, PKType: "serial"},
		"orders": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	rh := newTestReadHandler(prodConn, shadowConn, tables, t)
	// No deltas or tombstones — should fall back to Prod relay.

	// Prod responds to the forwarded query.
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"1", "alice"}},
		"SELECT 1",
	))

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"orders", "users"},
		IsJoin:  true,
		RawSQL:  "SELECT id, name FROM orders o JOIN users u ON o.user_id = u.id",
	}

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := rh.HandleRead(clientConn, nil, cl, core.StrategyJoinPatch)
	if err != nil {
		t.Fatalf("HandleRead() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}

	rowCount, _, err := parseClientResponse(clientBuf)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected 1 row (Prod fallback), got %d", rowCount)
	}
}

// --- Helper function tests ---

func TestRewriteLimit(t *testing.T) {
	tests := []struct {
		sql      string
		newLimit int
		want     string
	}{
		{"SELECT * FROM users LIMIT 10", 15, "SELECT * FROM users LIMIT 15"},
		{"SELECT * FROM users limit 5", 8, "SELECT * FROM users limit 8"},
		{"SELECT * FROM users LIMIT 10 OFFSET 5", 20, "SELECT * FROM users LIMIT 20 OFFSET 5"},
		{"SELECT * FROM users", 10, "SELECT * FROM users"},
	}

	for _, tt := range tests {
		got := rewriteLimit(tt.sql, tt.newLimit)
		if got != tt.want {
			t.Errorf("rewriteLimit(%q, %d) = %q, want %q", tt.sql, tt.newLimit, got, tt.want)
		}
	}
}

func TestParseSimpleOrderBy(t *testing.T) {
	tests := []struct {
		orderBy  string
		wantCol  string
		wantDesc bool
	}{
		{"id ASC", "id", false},
		{"id DESC", "id", true},
		{"name", "name", false},
		{"u.name DESC", "name", true},
		{`"created_at" DESC`, "created_at", true},
		{"id, name", "id", false}, // multi-column: returns first column
		{"", "", false},
	}

	for _, tt := range tests {
		col, desc := parseSimpleOrderBy(tt.orderBy)
		if col != tt.wantCol || desc != tt.wantDesc {
			t.Errorf("parseSimpleOrderBy(%q) = (%q, %v), want (%q, %v)",
				tt.orderBy, col, desc, tt.wantCol, tt.wantDesc)
		}
	}
}
