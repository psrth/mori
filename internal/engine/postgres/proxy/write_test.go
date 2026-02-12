package proxy

import (
	"io"
	"net"
	"sync"
	"testing"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
)

// mockPGBackend simulates a PG backend that reads a query and writes a response.
// It captures what was sent to it for assertions.
type mockPGBackend struct {
	conn     net.Conn // the "server" side of the pipe
	received [][]byte // captured raw messages
	mu       sync.Mutex
}

// respondWith makes the backend read one query message, then write the given
// response bytes. It captures the received message.
func (m *mockPGBackend) respondWith(response []byte) {
	go func() {
		msg, err := readMsg(m.conn)
		if err != nil {
			return
		}
		m.mu.Lock()
		m.received = append(m.received, msg.Raw)
		m.mu.Unlock()
		m.conn.Write(response) //nolint: errcheck
	}()
}

// respondToMultiple reads n query messages and responds to each with the
// corresponding response from the slice.
func (m *mockPGBackend) respondToMultiple(responses [][]byte) {
	go func() {
		for _, resp := range responses {
			msg, err := readMsg(m.conn)
			if err != nil {
				return
			}
			m.mu.Lock()
			m.received = append(m.received, msg.Raw)
			m.mu.Unlock()
			m.conn.Write(resp) //nolint: errcheck
		}
	}()
}

// getReceived returns captured messages.
func (m *mockPGBackend) getReceived() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([][]byte, len(m.received))
	copy(cp, m.received)
	return cp
}

// newMockBackend creates a net.Pipe and returns the client-side conn and the mock backend.
func newMockBackend() (clientSide net.Conn, backend *mockPGBackend) {
	c, s := net.Pipe()
	return c, &mockPGBackend{conn: s}
}

// simpleResponse builds a standard PG response: CommandComplete + ReadyForQuery.
func simpleResponse(tag string) []byte {
	var buf []byte
	buf = append(buf, makeCommandCompleteMsg(tag)...)
	buf = append(buf, makeReadyForQueryMsg()...)
	return buf
}

// selectResponse builds a PG response with RowDescription + DataRows + CommandComplete + ReadyForQuery.
func selectResponse(columns []string, rows [][]interface{}, tag string) []byte {
	var buf []byte
	buf = append(buf, makeRowDescMsg(columns...)...)
	for _, row := range rows {
		buf = append(buf, makeDataRowMsg(row...)...)
	}
	buf = append(buf, makeCommandCompleteMsg(tag)...)
	buf = append(buf, makeReadyForQueryMsg()...)
	return buf
}

// emptySelectResponse builds a PG response with RowDescription + CommandComplete (no rows).
func emptySelectResponse(columns []string) []byte {
	var buf []byte
	buf = append(buf, makeRowDescMsg(columns...)...)
	buf = append(buf, makeCommandCompleteMsg("SELECT 0")...)
	buf = append(buf, makeReadyForQueryMsg()...)
	return buf
}

// newTestWriteHandler creates a WriteHandler with mock connections and fresh state.
func newTestWriteHandler(
	prodConn, shadowConn net.Conn,
	tables map[string]schema.TableMeta,
	t *testing.T,
) *WriteHandler {
	return &WriteHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   delta.NewMap(),
		tombstones: delta.NewTombstoneSet(),
		tables:     tables,
		moriDir:    t.TempDir(),
		connID:     1,
		verbose:    false,
	}
}

// readRelayedResponse reads the full PG response relayed to the client
// (until ReadyForQuery 'Z').
func readRelayedResponse(conn net.Conn) ([]byte, error) {
	var buf []byte
	for {
		msg, err := readMsg(conn)
		if err != nil {
			return buf, err
		}
		buf = append(buf, msg.Raw...)
		if msg.Type == 'Z' {
			return buf, nil
		}
	}
}

func TestHandleInsert(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	wh := newTestWriteHandler(prodConn, shadowConn, nil, t)

	// Shadow responds with INSERT success.
	shadowBackend.respondWith(simpleResponse("INSERT 0 1"))

	rawMsg := buildQueryMsg("INSERT INTO users (name) VALUES ('alice')")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubInsert,
		Tables:  []string{"users"},
	}

	// Read the relayed response on the client side.
	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyShadowWrite)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}
	if len(clientBuf) == 0 {
		t.Error("expected non-empty response relayed to client")
	}

	// Delta map should NOT have any entries (INSERT doesn't track deltas).
	if wh.deltaMap.CountForTable("users") != 0 {
		t.Errorf("delta map should be empty for INSERT, got %d entries", wh.deltaMap.CountForTable("users"))
	}
}

func TestHandleUpdatePointWithHydration(t *testing.T) {
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
	wh := newTestWriteHandler(prodConn, shadowConn, tables, t)

	// Prod responds to hydration SELECT with one row.
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name", "email"},
		[][]interface{}{{"42", "alice", "alice@example.com"}},
		"SELECT 1",
	))

	// Shadow responds to hydration INSERT and then the UPDATE.
	shadowBackend.respondToMultiple([][]byte{
		simpleResponse("INSERT 0 1"),
		simpleResponse("UPDATE 1"),
	})

	// Read the client response.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("UPDATE users SET name = 'bob' WHERE id = 42")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "42"}},
	}

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyHydrateAndWrite)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()

	// Delta map should have (users, 42).
	if !wh.deltaMap.IsDelta("users", "42") {
		t.Error("expected (users, 42) in delta map after hydrate+update")
	}

	// Prod should have received exactly 1 message (the SELECT).
	received := prodBackend.getReceived()
	if len(received) != 1 {
		t.Errorf("prod received %d messages, want 1", len(received))
	}

	// Shadow should have received 2 messages (INSERT + UPDATE).
	shadowReceived := shadowBackend.getReceived()
	if len(shadowReceived) != 2 {
		t.Errorf("shadow received %d messages, want 2", len(shadowReceived))
	}
}

func TestHandleUpdatePointAlreadyDelta(t *testing.T) {
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
	wh := newTestWriteHandler(prodConn, shadowConn, tables, t)

	// Pre-populate delta map — row is already in Shadow.
	wh.deltaMap.Add("users", "42")

	// Shadow responds to the UPDATE only (no hydration needed).
	shadowBackend.respondWith(simpleResponse("UPDATE 1"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("UPDATE users SET name = 'bob' WHERE id = 42")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "42"}},
	}

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyHydrateAndWrite)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()

	// Prod should have received 0 messages (no hydration needed).
	received := prodBackend.getReceived()
	if len(received) != 0 {
		t.Errorf("prod received %d messages, want 0 (row already delta)", len(received))
	}

	// Shadow should have received 1 message (the UPDATE).
	shadowReceived := shadowBackend.getReceived()
	if len(shadowReceived) != 1 {
		t.Errorf("shadow received %d messages, want 1", len(shadowReceived))
	}
}

func TestHandleUpdateBulkFallback(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	wh := newTestWriteHandler(prodConn, shadowConn, nil, t)

	// Shadow responds to the UPDATE.
	shadowBackend.respondWith(simpleResponse("UPDATE 0"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	// No PKs — bulk update.
	rawMsg := buildQueryMsg("UPDATE users SET active = false WHERE last_login < '2020-01-01'")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     nil, // no PKs extractable
	}

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyHydrateAndWrite)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()

	// Prod should have received 0 messages (no hydration for bulk).
	received := prodBackend.getReceived()
	if len(received) != 0 {
		t.Errorf("prod received %d messages, want 0 (bulk update)", len(received))
	}

	// Delta map should be empty (no PKs to track).
	if wh.deltaMap.CountForTable("users") != 0 {
		t.Errorf("delta map should be empty for bulk update")
	}
}

func TestHandleUpdateHydrationProdRowNotFound(t *testing.T) {
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
	wh := newTestWriteHandler(prodConn, shadowConn, tables, t)

	// Prod returns 0 rows for the hydration SELECT.
	prodBackend.respondWith(emptySelectResponse([]string{"id", "name"}))

	// Shadow responds to the UPDATE (no hydration INSERT since row not in Prod).
	shadowBackend.respondWith(simpleResponse("UPDATE 0"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("UPDATE users SET name = 'bob' WHERE id = 999")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "999"}},
	}

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyHydrateAndWrite)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()

	// Delta map should still have the PK tracked.
	if !wh.deltaMap.IsDelta("users", "999") {
		t.Error("expected (users, 999) in delta map even when Prod row not found")
	}

	// Shadow should have received only 1 message (the UPDATE, no hydration INSERT).
	shadowReceived := shadowBackend.getReceived()
	if len(shadowReceived) != 1 {
		t.Errorf("shadow received %d messages, want 1", len(shadowReceived))
	}
}

func TestHandleDeleteWithTombstone(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	wh := newTestWriteHandler(prodConn, shadowConn, nil, t)

	// Pre-populate delta map with the row we're about to delete.
	wh.deltaMap.Add("users", "43")

	// Shadow responds to the DELETE.
	shadowBackend.respondWith(simpleResponse("DELETE 1"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("DELETE FROM users WHERE id = 43")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubDelete,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "43"}},
	}

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyShadowDelete)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()

	// Tombstone should have (users, 43).
	if !wh.tombstones.IsTombstoned("users", "43") {
		t.Error("expected (users, 43) in tombstone set")
	}

	// Delta map should no longer have (users, 43).
	if wh.deltaMap.IsDelta("users", "43") {
		t.Error("expected (users, 43) removed from delta map after delete")
	}

	// Prod should have received 0 messages.
	received := prodBackend.getReceived()
	if len(received) != 0 {
		t.Errorf("prod received %d messages, want 0", len(received))
	}
}

func TestHandleDeleteBulkFallback(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	wh := newTestWriteHandler(prodConn, shadowConn, nil, t)

	// Shadow responds to the DELETE.
	shadowBackend.respondWith(simpleResponse("DELETE 0"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("DELETE FROM users WHERE active = false")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubDelete,
		Tables:  []string{"users"},
		PKs:     nil, // no PKs extractable
	}

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyShadowDelete)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()

	// Tombstone set should be empty (no PKs to tombstone).
	if wh.tombstones.CountForTable("users") != 0 {
		t.Error("tombstone set should be empty for bulk delete without PKs")
	}
}

func TestHandleDeleteMultiplePKs(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	wh := newTestWriteHandler(prodConn, shadowConn, nil, t)

	// Pre-populate one as a delta.
	wh.deltaMap.Add("users", "10")

	shadowBackend.respondWith(simpleResponse("DELETE 2"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("DELETE FROM users WHERE id IN (10, 20)")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubDelete,
		Tables:  []string{"users"},
		PKs: []core.TablePK{
			{Table: "users", PK: "10"},
			{Table: "users", PK: "20"},
		},
	}

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyShadowDelete)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()

	// Both should be tombstoned.
	if !wh.tombstones.IsTombstoned("users", "10") {
		t.Error("expected (users, 10) tombstoned")
	}
	if !wh.tombstones.IsTombstoned("users", "20") {
		t.Error("expected (users, 20) tombstoned")
	}

	// Delta for 10 should be removed.
	if wh.deltaMap.IsDelta("users", "10") {
		t.Error("expected (users, 10) removed from delta map")
	}
}

func TestHandleWriteDispatch(t *testing.T) {
	// Test that HandleWrite returns an error for unsupported strategies.
	wh := &WriteHandler{deltaMap: delta.NewMap(), tombstones: delta.NewTombstoneSet()}
	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	// Drain client side to avoid blocking.
	go io.Copy(io.Discard, clientSide) //nolint: errcheck

	err := wh.HandleWrite(clientConn, nil, &core.Classification{}, core.StrategyProdDirect)
	if err == nil {
		t.Error("expected error for unsupported strategy")
	}
}

func TestHandleWritePersistence(t *testing.T) {
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
	wh := newTestWriteHandler(prodConn, shadowConn, tables, t)

	// Prod: no row found (skip hydration INSERT).
	prodBackend.respondWith(emptySelectResponse([]string{"id", "name"}))

	// Shadow: UPDATE.
	shadowBackend.respondWith(simpleResponse("UPDATE 1"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("UPDATE users SET name = 'x' WHERE id = 1")
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "1"}},
	}

	err := wh.HandleWrite(clientConn, rawMsg, cl, core.StrategyHydrateAndWrite)
	if err != nil {
		t.Fatalf("HandleWrite() error: %v", err)
	}

	wg.Wait()

	// Verify delta map was persisted by reading it back.
	dm, err := delta.ReadDeltaMap(wh.moriDir)
	if err != nil {
		t.Fatalf("ReadDeltaMap() error: %v", err)
	}
	if !dm.IsDelta("users", "1") {
		t.Error("persisted delta map should contain (users, 1)")
	}
}
