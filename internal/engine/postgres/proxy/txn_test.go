package proxy

import (
	"net"
	"sync"
	"testing"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
)

// newTestTxnHandler creates a TxnHandler with mock connections and fresh state.
func newTestTxnHandler(prodConn, shadowConn net.Conn, t *testing.T) *TxnHandler {
	return &TxnHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   delta.NewMap(),
		tombstones: delta.NewTombstoneSet(),
		moriDir:    t.TempDir(),
		connID:     1,
		verbose:    true,
	}
}

// --- TxnHandler unit tests ---

func TestHandleTxnBegin(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	txh := newTestTxnHandler(prodConn, shadowConn, t)

	// Shadow responds to BEGIN.
	shadowBackend.respondWith(simpleResponse("BEGIN"))
	// Prod responds to BEGIN ISOLATION LEVEL REPEATABLE READ.
	prodBackend.respondWith(simpleResponse("BEGIN"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("BEGIN")
	cl := &core.Classification{
		OpType:  core.OpTransaction,
		SubType: core.SubBegin,
		RawSQL:  "BEGIN",
	}

	err := txh.HandleTxn(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleTxn(BEGIN) error: %v", err)
	}
	wg.Wait()

	// Verify inTxn is set.
	if !txh.InTxn() {
		t.Error("expected InTxn() = true after BEGIN")
	}

	// Verify Prod received REPEATABLE READ (not the original BEGIN).
	prodReceived := prodBackend.getReceived()
	if len(prodReceived) != 1 {
		t.Fatalf("prod received %d messages, want 1", len(prodReceived))
	}
	prodSQL := querySQL(prodReceived[0][5:]) // skip type+length
	if prodSQL != "BEGIN ISOLATION LEVEL REPEATABLE READ" {
		t.Errorf("prod received SQL = %q, want %q", prodSQL, "BEGIN ISOLATION LEVEL REPEATABLE READ")
	}

	// Verify Shadow received the original BEGIN.
	shadowReceived := shadowBackend.getReceived()
	if len(shadowReceived) != 1 {
		t.Fatalf("shadow received %d messages, want 1", len(shadowReceived))
	}
	shadowSQL := querySQL(shadowReceived[0][5:])
	if shadowSQL != "BEGIN" {
		t.Errorf("shadow received SQL = %q, want %q", shadowSQL, "BEGIN")
	}
}

func TestHandleTxnCommit(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	txh := newTestTxnHandler(prodConn, shadowConn, t)
	txh.inTxn = true // Simulate being in a transaction.

	// Pre-stage some deltas.
	txh.deltaMap.Stage("users", "42")
	txh.tombstones.Stage("orders", "10")

	// Shadow responds to COMMIT.
	shadowBackend.respondWith(simpleResponse("COMMIT"))
	// Prod responds to COMMIT.
	prodBackend.respondWith(simpleResponse("COMMIT"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("COMMIT")
	cl := &core.Classification{
		OpType:  core.OpTransaction,
		SubType: core.SubCommit,
		RawSQL:  "COMMIT",
	}

	err := txh.HandleTxn(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleTxn(COMMIT) error: %v", err)
	}
	wg.Wait()

	// Verify inTxn is cleared.
	if txh.InTxn() {
		t.Error("expected InTxn() = false after COMMIT")
	}

	// Verify staged deltas were promoted.
	if !txh.deltaMap.IsDelta("users", "42") {
		t.Error("expected (users, 42) in delta map after COMMIT")
	}
	if !txh.tombstones.IsTombstoned("orders", "10") {
		t.Error("expected (orders, 10) in tombstone set after COMMIT")
	}

	// Verify committed (not just staged) — DeltaPKs only returns committed entries.
	pks := txh.deltaMap.DeltaPKs("users")
	if len(pks) != 1 || pks[0] != "42" {
		t.Errorf("DeltaPKs(users) = %v, want [42]", pks)
	}
}

func TestHandleTxnRollback(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	txh := newTestTxnHandler(prodConn, shadowConn, t)
	txh.inTxn = true

	// Pre-stage some deltas.
	txh.deltaMap.Stage("users", "42")
	txh.tombstones.Stage("orders", "10")

	// Shadow responds to ROLLBACK.
	shadowBackend.respondWith(simpleResponse("ROLLBACK"))
	// Prod responds to ROLLBACK.
	prodBackend.respondWith(simpleResponse("ROLLBACK"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("ROLLBACK")
	cl := &core.Classification{
		OpType:  core.OpTransaction,
		SubType: core.SubRollback,
		RawSQL:  "ROLLBACK",
	}

	err := txh.HandleTxn(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleTxn(ROLLBACK) error: %v", err)
	}
	wg.Wait()

	// Verify inTxn is cleared.
	if txh.InTxn() {
		t.Error("expected InTxn() = false after ROLLBACK")
	}

	// Verify staged deltas were discarded.
	if txh.deltaMap.IsDelta("users", "42") {
		t.Error("expected (users, 42) NOT in delta map after ROLLBACK")
	}
	if txh.tombstones.IsTombstoned("orders", "10") {
		t.Error("expected (orders, 10) NOT in tombstone set after ROLLBACK")
	}

	// DeltaPKs should be empty.
	if pks := txh.deltaMap.DeltaPKs("users"); len(pks) != 0 {
		t.Errorf("DeltaPKs(users) = %v, want empty", pks)
	}
}

func TestHandleTxnCommitPromotesDeltas(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	txh := newTestTxnHandler(prodConn, shadowConn, t)
	txh.inTxn = true

	// Stage multiple entries.
	txh.deltaMap.Stage("users", "1")
	txh.deltaMap.Stage("users", "2")
	txh.tombstones.Stage("orders", "100")

	// Staged entries are visible via IsDelta/IsTombstoned (via has()).
	if !txh.deltaMap.IsDelta("users", "1") {
		t.Error("staged (users, 1) should be visible via IsDelta")
	}

	// But not in DeltaPKs (committed only).
	if pks := txh.deltaMap.DeltaPKs("users"); len(pks) != 0 {
		t.Errorf("DeltaPKs before commit = %v, want empty", pks)
	}

	shadowBackend.respondWith(simpleResponse("COMMIT"))
	prodBackend.respondWith(simpleResponse("COMMIT"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	cl := &core.Classification{
		OpType:  core.OpTransaction,
		SubType: core.SubCommit,
		RawSQL:  "COMMIT",
	}
	err := txh.HandleTxn(clientConn, buildQueryMsg("COMMIT"), cl)
	if err != nil {
		t.Fatalf("HandleTxn(COMMIT) error: %v", err)
	}
	wg.Wait()

	// After commit, entries should be in committed state.
	pks := txh.deltaMap.DeltaPKs("users")
	if len(pks) != 2 {
		t.Fatalf("DeltaPKs(users) after commit = %v, want 2 entries", pks)
	}

	tpks := txh.tombstones.TombstonedPKs("orders")
	if len(tpks) != 1 || tpks[0] != "100" {
		t.Errorf("TombstonedPKs(orders) = %v, want [100]", tpks)
	}

	// Verify persistence.
	dm, err := delta.ReadDeltaMap(txh.moriDir)
	if err != nil {
		t.Fatalf("ReadDeltaMap() error: %v", err)
	}
	if !dm.IsDelta("users", "1") || !dm.IsDelta("users", "2") {
		t.Error("persisted delta map should contain (users, 1) and (users, 2)")
	}
}

func TestHandleTxnRollbackDiscardsDeltas(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	txh := newTestTxnHandler(prodConn, shadowConn, t)
	txh.inTxn = true

	// Stage entries.
	txh.deltaMap.Stage("users", "1")
	txh.deltaMap.Stage("users", "2")
	txh.tombstones.Stage("orders", "100")

	// Also add a committed entry that should NOT be affected.
	txh.deltaMap.Add("products", "50")

	shadowBackend.respondWith(simpleResponse("ROLLBACK"))
	prodBackend.respondWith(simpleResponse("ROLLBACK"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	cl := &core.Classification{
		OpType:  core.OpTransaction,
		SubType: core.SubRollback,
		RawSQL:  "ROLLBACK",
	}
	err := txh.HandleTxn(clientConn, buildQueryMsg("ROLLBACK"), cl)
	if err != nil {
		t.Fatalf("HandleTxn(ROLLBACK) error: %v", err)
	}
	wg.Wait()

	// Staged entries should be gone.
	if txh.deltaMap.IsDelta("users", "1") {
		t.Error("expected (users, 1) gone after ROLLBACK")
	}
	if txh.deltaMap.IsDelta("users", "2") {
		t.Error("expected (users, 2) gone after ROLLBACK")
	}
	if txh.tombstones.IsTombstoned("orders", "100") {
		t.Error("expected (orders, 100) gone after ROLLBACK")
	}

	// Committed entry should still be there.
	if !txh.deltaMap.IsDelta("products", "50") {
		t.Error("expected committed (products, 50) to survive ROLLBACK")
	}
}

func TestHandleTxnSavepoint(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	txh := newTestTxnHandler(prodConn, shadowConn, t)
	txh.inTxn = true

	// Shadow responds to SAVEPOINT.
	shadowBackend.respondWith(simpleResponse("SAVEPOINT"))
	// Prod responds to SAVEPOINT.
	prodBackend.respondWith(simpleResponse("SAVEPOINT"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("SAVEPOINT my_save")
	cl := &core.Classification{
		OpType:  core.OpTransaction,
		SubType: core.SubOther,
		RawSQL:  "SAVEPOINT my_save",
	}

	err := txh.HandleTxn(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleTxn(SAVEPOINT) error: %v", err)
	}
	wg.Wait()

	// inTxn should remain true.
	if !txh.InTxn() {
		t.Error("expected InTxn() = true after SAVEPOINT (no state change)")
	}
}

// --- Integration tests: TxnHandler + WriteHandler ---

func TestWriteHandlerStagesDuringTxn(t *testing.T) {
	// Setup Shadow and Prod mock backends.
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

	txh := &TxnHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   delta.NewMap(),
		tombstones: delta.NewTombstoneSet(),
		moriDir:    t.TempDir(),
		connID:     1,
		verbose:    true,
	}

	wh := &WriteHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   txh.deltaMap,
		tombstones: txh.tombstones,
		tables:     tables,
		moriDir:    txh.moriDir,
		connID:     1,
		verbose:    false,
		txnHandler: txh,
	}

	// --- Step 1: BEGIN ---
	shadowBackend.respondWith(simpleResponse("BEGIN"))
	prodBackend.respondWith(simpleResponse("BEGIN"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	beginCl := &core.Classification{
		OpType:  core.OpTransaction,
		SubType: core.SubBegin,
		RawSQL:  "BEGIN",
	}
	if err := txh.HandleTxn(clientConn, buildQueryMsg("BEGIN"), beginCl); err != nil {
		t.Fatalf("BEGIN error: %v", err)
	}
	wg.Wait()

	if !txh.InTxn() {
		t.Fatal("expected InTxn() = true")
	}

	// --- Step 2: UPDATE (within txn) ---
	// Prod responds to hydration SELECT: row not found (skip hydration).
	prodBackend.respondWith(emptySelectResponse([]string{"id", "name"}))
	// Shadow responds to UPDATE.
	shadowBackend.respondWith(simpleResponse("UPDATE 1"))

	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	updateCl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "42"}},
	}
	if err := wh.HandleWrite(clientConn, buildQueryMsg("UPDATE users SET name='x' WHERE id=42"), updateCl, core.StrategyHydrateAndWrite); err != nil {
		t.Fatalf("UPDATE error: %v", err)
	}
	wg.Wait()

	// Delta should be staged (visible via IsDelta) but not committed (not in DeltaPKs).
	if !wh.deltaMap.IsDelta("users", "42") {
		t.Error("expected (users, 42) visible via IsDelta during txn")
	}
	if pks := wh.deltaMap.DeltaPKs("users"); len(pks) != 0 {
		t.Errorf("DeltaPKs should be empty during txn, got %v", pks)
	}

	// --- Step 3: COMMIT ---
	shadowBackend.respondWith(simpleResponse("COMMIT"))
	prodBackend.respondWith(simpleResponse("COMMIT"))

	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	commitCl := &core.Classification{
		OpType:  core.OpTransaction,
		SubType: core.SubCommit,
		RawSQL:  "COMMIT",
	}
	if err := txh.HandleTxn(clientConn, buildQueryMsg("COMMIT"), commitCl); err != nil {
		t.Fatalf("COMMIT error: %v", err)
	}
	wg.Wait()

	// After commit, delta should be promoted.
	pks := wh.deltaMap.DeltaPKs("users")
	if len(pks) != 1 || pks[0] != "42" {
		t.Errorf("DeltaPKs after commit = %v, want [42]", pks)
	}
}

func TestWriteHandlerDeleteStagesDuringTxn(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	txh := &TxnHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   delta.NewMap(),
		tombstones: delta.NewTombstoneSet(),
		moriDir:    t.TempDir(),
		connID:     1,
		verbose:    true,
	}

	wh := &WriteHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   txh.deltaMap,
		tombstones: txh.tombstones,
		tables:     nil,
		moriDir:    txh.moriDir,
		connID:     1,
		verbose:    false,
		txnHandler: txh,
	}

	// --- BEGIN ---
	shadowBackend.respondWith(simpleResponse("BEGIN"))
	prodBackend.respondWith(simpleResponse("BEGIN"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	beginCl := &core.Classification{OpType: core.OpTransaction, SubType: core.SubBegin, RawSQL: "BEGIN"}
	if err := txh.HandleTxn(clientConn, buildQueryMsg("BEGIN"), beginCl); err != nil {
		t.Fatalf("BEGIN error: %v", err)
	}
	wg.Wait()

	// --- DELETE (within txn) ---
	shadowBackend.respondWith(simpleResponse("DELETE 1"))

	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	deleteCl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubDelete,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "43"}},
	}
	if err := wh.HandleWrite(clientConn, buildQueryMsg("DELETE FROM users WHERE id = 43"), deleteCl, core.StrategyShadowDelete); err != nil {
		t.Fatalf("DELETE error: %v", err)
	}
	wg.Wait()

	// Tombstone should be staged (visible via IsTombstoned).
	if !wh.tombstones.IsTombstoned("users", "43") {
		t.Error("expected (users, 43) visible via IsTombstoned during txn")
	}
	// But not in committed TombstonedPKs.
	if tpks := wh.tombstones.TombstonedPKs("users"); len(tpks) != 0 {
		t.Errorf("TombstonedPKs should be empty during txn, got %v", tpks)
	}

	// --- COMMIT ---
	shadowBackend.respondWith(simpleResponse("COMMIT"))
	prodBackend.respondWith(simpleResponse("COMMIT"))

	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	commitCl := &core.Classification{OpType: core.OpTransaction, SubType: core.SubCommit, RawSQL: "COMMIT"}
	if err := txh.HandleTxn(clientConn, buildQueryMsg("COMMIT"), commitCl); err != nil {
		t.Fatalf("COMMIT error: %v", err)
	}
	wg.Wait()

	// After commit, tombstone should be promoted.
	tpks := wh.tombstones.TombstonedPKs("users")
	if len(tpks) != 1 || tpks[0] != "43" {
		t.Errorf("TombstonedPKs after commit = %v, want [43]", tpks)
	}
}

func TestWriteHandlerRollbackAfterWrite(t *testing.T) {
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

	txh := &TxnHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   delta.NewMap(),
		tombstones: delta.NewTombstoneSet(),
		moriDir:    t.TempDir(),
		connID:     1,
		verbose:    true,
	}

	wh := &WriteHandler{
		prodConn:   prodConn,
		shadowConn: shadowConn,
		deltaMap:   txh.deltaMap,
		tombstones: txh.tombstones,
		tables:     tables,
		moriDir:    txh.moriDir,
		connID:     1,
		verbose:    false,
		txnHandler: txh,
	}

	// --- BEGIN ---
	shadowBackend.respondWith(simpleResponse("BEGIN"))
	prodBackend.respondWith(simpleResponse("BEGIN"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	beginCl := &core.Classification{OpType: core.OpTransaction, SubType: core.SubBegin, RawSQL: "BEGIN"}
	if err := txh.HandleTxn(clientConn, buildQueryMsg("BEGIN"), beginCl); err != nil {
		t.Fatalf("BEGIN error: %v", err)
	}
	wg.Wait()

	// --- UPDATE (within txn) ---
	prodBackend.respondWith(emptySelectResponse([]string{"id", "name"}))
	shadowBackend.respondWith(simpleResponse("UPDATE 1"))

	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	updateCl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "42"}},
	}
	if err := wh.HandleWrite(clientConn, buildQueryMsg("UPDATE users SET name='x' WHERE id=42"), updateCl, core.StrategyHydrateAndWrite); err != nil {
		t.Fatalf("UPDATE error: %v", err)
	}
	wg.Wait()

	// Verify delta is staged during txn.
	if !wh.deltaMap.IsDelta("users", "42") {
		t.Error("expected (users, 42) visible during txn")
	}

	// --- ROLLBACK ---
	shadowBackend.respondWith(simpleResponse("ROLLBACK"))
	prodBackend.respondWith(simpleResponse("ROLLBACK"))

	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rollbackCl := &core.Classification{OpType: core.OpTransaction, SubType: core.SubRollback, RawSQL: "ROLLBACK"}
	if err := txh.HandleTxn(clientConn, buildQueryMsg("ROLLBACK"), rollbackCl); err != nil {
		t.Fatalf("ROLLBACK error: %v", err)
	}
	wg.Wait()

	// After rollback, delta should be gone.
	if wh.deltaMap.IsDelta("users", "42") {
		t.Error("expected (users, 42) gone after ROLLBACK")
	}
	if pks := wh.deltaMap.DeltaPKs("users"); len(pks) != 0 {
		t.Errorf("DeltaPKs after rollback = %v, want empty", pks)
	}
}
