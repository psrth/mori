package proxy

import (
	"net"
	"sync"
	"testing"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
)

// mockClassifier implements core.Classifier for tests.
type mockClassifier struct {
	cl  *core.Classification
	err error
}

func (mc *mockClassifier) Classify(_ string) (*core.Classification, error) {
	return mc.cl, mc.err
}

func (mc *mockClassifier) ClassifyWithParams(_ string, _ []interface{}) (*core.Classification, error) {
	return mc.cl, mc.err
}

// respondToBatch reads messages from the backend until Sync ('S'), then writes
// the response. This simulates a PG backend handling an extended query batch.
func (m *mockPGBackend) respondToBatch(response []byte) {
	go func() {
		for {
			msg, err := readMsg(m.conn)
			if err != nil {
				return
			}
			m.mu.Lock()
			m.received = append(m.received, msg.Raw)
			m.mu.Unlock()
			if msg.Type == 'S' { // Sync
				m.conn.Write(response) //nolint: errcheck
				return
			}
		}
	}()
}

// extResponse builds a standard extended protocol response:
// ParseComplete + BindComplete + CommandComplete + ReadyForQuery.
func extResponse(tag string) []byte {
	var buf []byte
	buf = append(buf, makeParseCompleteMsg()...)
	buf = append(buf, makeBindCompleteMsg()...)
	buf = append(buf, makeCommandCompleteMsg(tag)...)
	buf = append(buf, makeReadyForQueryMsg()...)
	return buf
}

// extSelectResponse builds an extended protocol SELECT response:
// ParseComplete + BindComplete + RowDescription + DataRows + CommandComplete + ReadyForQuery.
func extSelectResponse(columns []string, rows [][]interface{}, tag string) []byte {
	var buf []byte
	buf = append(buf, makeParseCompleteMsg()...)
	buf = append(buf, makeBindCompleteMsg()...)
	buf = append(buf, makeRowDescMsg(columns...)...)
	for _, row := range rows {
		buf = append(buf, makeDataRowMsg(row...)...)
	}
	buf = append(buf, makeCommandCompleteMsg(tag)...)
	buf = append(buf, makeReadyForQueryMsg()...)
	return buf
}

// newTestExtHandler creates an ExtHandler with mock connections and fresh state.
func newTestExtHandler(
	prodConn, shadowConn net.Conn,
	classifier core.Classifier,
	cl *core.Classification,
	tables map[string]schema.TableMeta,
	t *testing.T,
) *ExtHandler {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()
	router := core.NewRouter(dm, ts, nil)

	eh := &ExtHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		classifier: classifier,
		router:     router,
		deltaMap:   dm,
		tombstones: ts,
		tables:     tables,
		moriDir:    t.TempDir(),
		connID:     1,
		verbose:    false,
		stmtCache:  make(map[string]string),
	}
	return eh
}

// feedBatch accumulates a Parse+Bind+Execute+Sync batch into the ExtHandler.
func feedBatch(eh *ExtHandler, stmtName, sql string, params ...string) {
	parseMsg := makePgMsg('P', makeParseMsg(stmtName, sql))
	eh.Accumulate(parseMsg)

	bindMsg := makePgMsg('B', makeBindMsg("", stmtName, params...))
	eh.Accumulate(bindMsg)

	execMsg := makePgMsg('E', makeExecuteMsg(""))
	eh.Accumulate(execMsg)

	syncMsg := makePgMsg('S', makeSyncMsg())
	eh.Accumulate(syncMsg)
}

// makePgMsg converts raw wire bytes into a *pgMsg struct.
func makePgMsg(msgType byte, raw []byte) *pgMsg {
	return &pgMsg{
		Type:    msgType,
		Payload: raw[5:],
		Raw:     raw,
	}
}

// --- ExtHandler Tests ---

func TestExtProdDirect(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT * FROM users WHERE id = $1",
	}
	classifier := &mockClassifier{cl: cl}
	eh := newTestExtHandler(prodConn, shadowConn, classifier, cl, nil, t)

	// Prod responds to the batch.
	prodBackend.respondToBatch(extSelectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"42", "alice"}},
		"SELECT 1",
	))

	// Read the relayed response on the client side.
	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	// Feed the batch and flush.
	feedBatch(eh, "", "SELECT * FROM users WHERE id = $1", "42")
	err := eh.FlushBatch(clientConn)
	if err != nil {
		t.Fatalf("FlushBatch() error: %v", err)
	}

	wg.Wait()
	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}
	if len(clientBuf) == 0 {
		t.Error("expected non-empty response relayed to client")
	}

	// Should have been sent to Prod (ProdDirect — no deltas for 'users').
	received := prodBackend.getReceived()
	if len(received) == 0 {
		t.Error("prod should have received batch messages")
	}

	// Shadow should have received nothing.
	shadowReceived := shadowBackend.getReceived()
	if len(shadowReceived) != 0 {
		t.Errorf("shadow received %d messages, want 0", len(shadowReceived))
	}
}

func TestExtInsert(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubInsert,
		Tables:  []string{"users"},
		RawSQL:  "INSERT INTO users (name) VALUES ($1)",
	}
	classifier := &mockClassifier{cl: cl}
	eh := newTestExtHandler(prodConn, shadowConn, classifier, cl, nil, t)

	// Shadow responds to the batch.
	shadowBackend.respondToBatch(extResponse("INSERT 0 1"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	feedBatch(eh, "", "INSERT INTO users (name) VALUES ($1)", "alice")
	err := eh.FlushBatch(clientConn)
	if err != nil {
		t.Fatalf("FlushBatch() error: %v", err)
	}

	wg.Wait()

	// Shadow should have received the batch.
	shadowReceived := shadowBackend.getReceived()
	if len(shadowReceived) == 0 {
		t.Error("shadow should have received batch messages")
	}

	// Prod should not have received anything.
	prodReceived := prodBackend.getReceived()
	if len(prodReceived) != 0 {
		t.Errorf("prod received %d messages, want 0", len(prodReceived))
	}

	// Table should be marked as having inserts.
	if !eh.deltaMap.HasInserts("users") {
		t.Error("delta map should mark users as having inserts")
	}
}

func TestExtDelete(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubDelete,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "42"}},
		RawSQL:  "DELETE FROM users WHERE id = $1",
	}
	classifier := &mockClassifier{cl: cl}
	eh := newTestExtHandler(prodConn, shadowConn, classifier, cl, nil, t)

	// Shadow responds to the batch.
	shadowBackend.respondToBatch(extResponse("DELETE 1"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	feedBatch(eh, "", "DELETE FROM users WHERE id = $1", "42")
	err := eh.FlushBatch(clientConn)
	if err != nil {
		t.Fatalf("FlushBatch() error: %v", err)
	}

	wg.Wait()

	// Tombstone should have (users, 42).
	if !eh.tombstones.IsTombstoned("users", "42") {
		t.Error("expected (users, 42) in tombstone set")
	}

	// Shadow should have received the batch.
	shadowReceived := shadowBackend.getReceived()
	if len(shadowReceived) == 0 {
		t.Error("shadow should have received batch messages")
	}
}

func TestExtUpdate(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "42"}},
		RawSQL:  "UPDATE users SET name = $1 WHERE id = $2",
	}
	classifier := &mockClassifier{cl: cl}
	eh := newTestExtHandler(prodConn, shadowConn, classifier, cl, tables, t)

	// Set up WriteHandler for hydration.
	eh.writeHandler = &WriteHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   eh.deltaMap,
		tombstones: eh.tombstones,
		tables:     tables,
		moriDir:    eh.moriDir,
		connID:     1,
		verbose:    false,
	}

	// Prod responds to hydration SELECT.
	prodBackend.respondWith(selectResponse(
		[]string{"id", "name", "email"},
		[][]interface{}{{"42", "alice", "alice@example.com"}},
		"SELECT 1",
	))

	// Shadow handles: (1) hydration INSERT (simple query), (2) extended batch until Sync.
	go func() {
		// First: read the hydration INSERT (simple query 'Q').
		msg, err := readMsg(shadowBackend.conn)
		if err != nil {
			return
		}
		shadowBackend.mu.Lock()
		shadowBackend.received = append(shadowBackend.received, msg.Raw)
		shadowBackend.mu.Unlock()
		shadowBackend.conn.Write(simpleResponse("INSERT 0 1")) //nolint: errcheck

		// Second: read the extended batch until Sync.
		for {
			msg, err = readMsg(shadowBackend.conn)
			if err != nil {
				return
			}
			shadowBackend.mu.Lock()
			shadowBackend.received = append(shadowBackend.received, msg.Raw)
			shadowBackend.mu.Unlock()
			if msg.Type == 'S' {
				shadowBackend.conn.Write(extResponse("UPDATE 1")) //nolint: errcheck
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	feedBatch(eh, "", "UPDATE users SET name = $1 WHERE id = $2", "bob", "42")
	err := eh.FlushBatch(clientConn)
	if err != nil {
		t.Fatalf("FlushBatch() error: %v", err)
	}

	wg.Wait()

	// Delta map should have (users, 42).
	if !eh.deltaMap.IsDelta("users", "42") {
		t.Error("expected (users, 42) in delta map after hydrate+update")
	}

	// Prod should have received hydration SELECT.
	prodReceived := prodBackend.getReceived()
	if len(prodReceived) != 1 {
		t.Errorf("prod received %d messages, want 1 (hydration SELECT)", len(prodReceived))
	}
}

func TestExtUpdateAlreadyDelta(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "42"}},
		RawSQL:  "UPDATE users SET name = $1 WHERE id = $2",
	}
	classifier := &mockClassifier{cl: cl}
	eh := newTestExtHandler(prodConn, shadowConn, classifier, cl, tables, t)

	// Pre-populate delta — row already in Shadow.
	eh.deltaMap.Add("users", "42")

	eh.writeHandler = &WriteHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   eh.deltaMap,
		tombstones: eh.tombstones,
		tables:     tables,
		moriDir:    eh.moriDir,
		connID:     1,
		verbose:    false,
	}

	// Shadow responds to the batch only (no hydration).
	shadowBackend.respondToBatch(extResponse("UPDATE 1"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	feedBatch(eh, "", "UPDATE users SET name = $1 WHERE id = $2", "bob", "42")
	err := eh.FlushBatch(clientConn)
	if err != nil {
		t.Fatalf("FlushBatch() error: %v", err)
	}

	wg.Wait()

	// Prod should have received 0 messages (no hydration).
	prodReceived := prodBackend.getReceived()
	if len(prodReceived) != 0 {
		t.Errorf("prod received %d messages, want 0 (row already delta)", len(prodReceived))
	}

	// Delta should still be tracked.
	if !eh.deltaMap.IsDelta("users", "42") {
		t.Error("expected (users, 42) still in delta map")
	}
}

func TestExtStmtCache(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
		RawSQL:  "SELECT * FROM users WHERE id = $1",
	}
	classifier := &mockClassifier{cl: cl}
	eh := newTestExtHandler(prodConn, shadowConn, classifier, cl, nil, t)

	// Batch 1: Parse only (no Execute) — forwards to both backends.
	parseRaw := makeParseMsg("my_stmt", "SELECT * FROM users WHERE id = $1")
	eh.Accumulate(makePgMsg('P', parseRaw))
	syncRaw := makeSyncMsg()
	eh.Accumulate(makePgMsg('S', syncRaw))

	parseOnlyResp := func() []byte {
		var buf []byte
		buf = append(buf, makeParseCompleteMsg()...)
		buf = append(buf, makeReadyForQueryMsg()...)
		return buf
	}()

	// Shadow responds to the Parse-only batch (so drainUntilReady succeeds).
	shadowBackend.respondToBatch(parseOnlyResp)

	// Prod responds to the Parse-only batch.
	prodBackend.respondToBatch(parseOnlyResp)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	err := eh.FlushBatch(clientConn)
	if err != nil {
		t.Fatalf("FlushBatch() batch 1 error: %v", err)
	}
	wg.Wait()

	// Verify statement is cached.
	if sql, ok := eh.stmtCache["my_stmt"]; !ok {
		t.Error("expected my_stmt in stmtCache after Parse")
	} else if sql != "SELECT * FROM users WHERE id = $1" {
		t.Errorf("stmtCache[my_stmt] = %q, want %q", sql, "SELECT * FROM users WHERE id = $1")
	}

	// Batch 2: Bind + Execute (no Parse) — should look up SQL from cache.
	bindRaw := makeBindMsg("", "my_stmt", "42")
	eh.Accumulate(makePgMsg('B', bindRaw))
	execRaw := makeExecuteMsg("")
	eh.Accumulate(makePgMsg('E', execRaw))
	syncRaw2 := makeSyncMsg()
	eh.Accumulate(makePgMsg('S', syncRaw2))

	// Prod responds to the Bind+Execute batch.
	prodBackend.respondToBatch(extSelectResponse(
		[]string{"id", "name"},
		[][]interface{}{{"42", "alice"}},
		"SELECT 1",
	))

	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	err = eh.FlushBatch(clientConn)
	if err != nil {
		t.Fatalf("FlushBatch() batch 2 error: %v", err)
	}
	wg.Wait()

	// Verify SQL was found from cache (batch should have been classified and routed).
	if eh.batchSQL != "" {
		t.Error("batchSQL should be cleared after FlushBatch")
	}
}

func TestExtNoExecute(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	// Even though classification says write, no Execute means forward to Prod.
	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubInsert,
		Tables:  []string{"users"},
		RawSQL:  "INSERT INTO users (name) VALUES ($1)",
	}
	classifier := &mockClassifier{cl: cl}
	eh := newTestExtHandler(prodConn, shadowConn, classifier, cl, nil, t)

	// Describe-only batch: Parse + Describe + Sync (no Execute).
	parseRaw := makeParseMsg("", "INSERT INTO users (name) VALUES ($1)")
	eh.Accumulate(makePgMsg('P', parseRaw))
	descRaw := makeDescribeMsg('S', "")
	eh.Accumulate(makePgMsg('D', descRaw))
	syncRaw := makeSyncMsg()
	eh.Accumulate(makePgMsg('S', syncRaw))

	parseDescResp := func() []byte {
		var buf []byte
		buf = append(buf, makeParseCompleteMsg()...)
		buf = append(buf, buildRawMsg('n', nil)...) // NoData
		buf = append(buf, makeReadyForQueryMsg()...)
		return buf
	}()

	// Shadow responds to Parse-only (so drainUntilReady succeeds).
	shadowBackend.respondToBatch(parseDescResp)

	// Prod responds (forward because no Execute).
	prodBackend.respondToBatch(func() []byte {
		var buf []byte
		buf = append(buf, makeParseCompleteMsg()...)
		// ParameterDescription would go here in real PG, but we skip for tests.
		buf = append(buf, buildRawMsg('n', nil)...) // NoData
		buf = append(buf, makeReadyForQueryMsg()...)
		return buf
	}())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	err := eh.FlushBatch(clientConn)
	if err != nil {
		t.Fatalf("FlushBatch() error: %v", err)
	}
	wg.Wait()

	// Should have gone to Prod (no Execute → safe default).
	prodReceived := prodBackend.getReceived()
	if len(prodReceived) == 0 {
		t.Error("prod should have received batch messages (Describe-only)")
	}

	// Shadow should have received the batch too (Parse forwarded to both backends).
	shadowReceived := shadowBackend.getReceived()
	if len(shadowReceived) == 0 {
		t.Error("shadow should have received Parse batch (forwarded to both backends)")
	}
}

func TestExtCloseEvictsCache(t *testing.T) {
	eh := &ExtHandler{
		stmtCache: make(map[string]string),
	}

	// Populate cache via Parse.
	parseRaw := makeParseMsg("s1", "SELECT 1")
	eh.Accumulate(makePgMsg('P', parseRaw))

	if _, ok := eh.stmtCache["s1"]; !ok {
		t.Fatal("expected s1 in stmtCache after Parse")
	}

	// Close('S', "s1") should evict.
	closeRaw := makeCloseMsg('S', "s1")
	eh.Accumulate(makePgMsg('C', closeRaw))

	if _, ok := eh.stmtCache["s1"]; ok {
		t.Error("expected s1 evicted from stmtCache after Close")
	}
}

func TestExtAccumulateExtractsInfo(t *testing.T) {
	eh := &ExtHandler{
		stmtCache: make(map[string]string),
	}

	// Parse.
	parseRaw := makeParseMsg("", "SELECT * FROM t WHERE a = $1")
	eh.Accumulate(makePgMsg('P', parseRaw))
	if !eh.batchHasParse {
		t.Error("batchHasParse should be true after Parse")
	}
	if eh.batchSQL != "SELECT * FROM t WHERE a = $1" {
		t.Errorf("batchSQL = %q, want %q", eh.batchSQL, "SELECT * FROM t WHERE a = $1")
	}

	// Bind.
	bindRaw := makeBindMsg("", "", "42")
	eh.Accumulate(makePgMsg('B', bindRaw))
	if !eh.batchHasBind {
		t.Error("batchHasBind should be true after Bind")
	}
	if len(eh.batchParams) != 1 {
		t.Fatalf("batchParams len = %d, want 1", len(eh.batchParams))
	}
	if string(eh.batchParams[0]) != "42" {
		t.Errorf("batchParams[0] = %q, want %q", eh.batchParams[0], "42")
	}

	// Describe.
	descRaw := makeDescribeMsg('P', "")
	eh.Accumulate(makePgMsg('D', descRaw))
	if !eh.batchHasDesc {
		t.Error("batchHasDesc should be true after Describe")
	}

	// Execute.
	execRaw := makeExecuteMsg("")
	eh.Accumulate(makePgMsg('E', execRaw))
	if !eh.batchHasExec {
		t.Error("batchHasExec should be true after Execute")
	}

	// Clear.
	eh.clearBatch()
	if eh.batchHasParse || eh.batchHasBind || eh.batchHasDesc || eh.batchHasExec {
		t.Error("batch flags should be false after clearBatch")
	}
	if eh.batchSQL != "" {
		t.Error("batchSQL should be empty after clearBatch")
	}
	if eh.batchParams != nil {
		t.Error("batchParams should be nil after clearBatch")
	}
}

func TestExtBindLookupFromCache(t *testing.T) {
	eh := &ExtHandler{
		stmtCache: map[string]string{
			"cached_stmt": "SELECT * FROM cached WHERE id = $1",
		},
	}

	// Bind references a cached statement (no Parse in this batch).
	bindRaw := makeBindMsg("", "cached_stmt", "99")
	eh.Accumulate(makePgMsg('B', bindRaw))

	if eh.batchSQL != "SELECT * FROM cached WHERE id = $1" {
		t.Errorf("batchSQL = %q, want %q", eh.batchSQL, "SELECT * FROM cached WHERE id = $1")
	}
}
