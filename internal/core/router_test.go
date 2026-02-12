package core

import (
	"testing"

	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
)

func TestNewRouterNotNil(t *testing.T) {
	r := NewRouter(nil, nil, nil)
	if r == nil {
		t.Fatal("NewRouter() returned nil")
	}
}

func TestRouterNilStateReadsAreProdDirect(t *testing.T) {
	r := NewRouter(nil, nil, nil)
	cl := &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"users"}}
	if got := r.Route(cl); got != StrategyProdDirect {
		t.Errorf("nil state SELECT: got %s, want PROD_DIRECT", got)
	}
}

func TestRouterNilStateWritesRouteToShadow(t *testing.T) {
	r := NewRouter(nil, nil, nil)
	cl := &Classification{OpType: OpWrite, SubType: SubInsert, Tables: []string{"users"}}
	if got := r.Route(cl); got != StrategyShadowWrite {
		t.Errorf("nil state INSERT: got %s, want SHADOW_WRITE", got)
	}
}

func TestRouterRoutingTable(t *testing.T) {
	dm := delta.NewMap()
	dm.Add("users", "42")

	ts := delta.NewTombstoneSet()
	ts.Add("orders", "99")

	r := NewRouter(dm, ts, nil)

	tests := []struct {
		name string
		cl   *Classification
		want RoutingStrategy
	}{
		// Reads on clean tables → ProdDirect
		{
			name: "SELECT clean table",
			cl:   &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"products"}},
			want: StrategyProdDirect,
		},
		// Reads on delta table → MergedRead
		{
			name: "SELECT delta table",
			cl:   &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"users"}},
			want: StrategyMergedRead,
		},
		// Reads on tombstone table → MergedRead
		{
			name: "SELECT tombstone table",
			cl:   &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"orders"}},
			want: StrategyMergedRead,
		},
		// JOIN on clean tables → ProdDirect
		{
			name: "SELECT JOIN clean",
			cl:   &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"products", "categories"}, IsJoin: true},
			want: StrategyProdDirect,
		},
		// JOIN where one table has deltas → JoinPatch
		{
			name: "SELECT JOIN delta",
			cl:   &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"users", "products"}, IsJoin: true},
			want: StrategyJoinPatch,
		},
		// JOIN where one table has tombstones → JoinPatch
		{
			name: "SELECT JOIN tombstone",
			cl:   &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"orders", "products"}, IsJoin: true},
			want: StrategyJoinPatch,
		},
		// INSERT → ShadowWrite
		{
			name: "INSERT",
			cl:   &Classification{OpType: OpWrite, SubType: SubInsert, Tables: []string{"users"}},
			want: StrategyShadowWrite,
		},
		// UPDATE → HydrateAndWrite
		{
			name: "UPDATE",
			cl:   &Classification{OpType: OpWrite, SubType: SubUpdate, Tables: []string{"users"}},
			want: StrategyHydrateAndWrite,
		},
		// DELETE → ShadowDelete
		{
			name: "DELETE",
			cl:   &Classification{OpType: OpWrite, SubType: SubDelete, Tables: []string{"users"}},
			want: StrategyShadowDelete,
		},
		// DDL: ALTER → ShadowDDL
		{
			name: "ALTER TABLE",
			cl:   &Classification{OpType: OpDDL, SubType: SubAlter, Tables: []string{"users"}},
			want: StrategyShadowDDL,
		},
		// DDL: CREATE → ShadowDDL
		{
			name: "CREATE TABLE",
			cl:   &Classification{OpType: OpDDL, SubType: SubCreate, Tables: []string{"new_table"}},
			want: StrategyShadowDDL,
		},
		// DDL: DROP → ShadowDDL
		{
			name: "DROP TABLE",
			cl:   &Classification{OpType: OpDDL, SubType: SubDrop, Tables: []string{"users"}},
			want: StrategyShadowDDL,
		},
		// Transaction: BEGIN
		{
			name: "BEGIN",
			cl:   &Classification{OpType: OpTransaction, SubType: SubBegin},
			want: StrategyTransaction,
		},
		// Transaction: COMMIT
		{
			name: "COMMIT",
			cl:   &Classification{OpType: OpTransaction, SubType: SubCommit},
			want: StrategyTransaction,
		},
		// Transaction: ROLLBACK
		{
			name: "ROLLBACK",
			cl:   &Classification{OpType: OpTransaction, SubType: SubRollback},
			want: StrategyTransaction,
		},
		// Other: SET → ProdDirect
		{
			name: "SET",
			cl:   &Classification{OpType: OpOther, SubType: SubOther},
			want: StrategyProdDirect,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.Route(tt.cl)
			if got != tt.want {
				t.Errorf("Route() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestRouterSelectCleanTableAfterDeltaRemoved(t *testing.T) {
	dm := delta.NewMap()
	dm.Add("users", "42")
	dm.Remove("users", "42")
	ts := delta.NewTombstoneSet()
	r := NewRouter(dm, ts, nil)

	cl := &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"users"}}
	if got := r.Route(cl); got != StrategyProdDirect {
		t.Errorf("SELECT after delta removed: got %s, want PROD_DIRECT", got)
	}
}

func TestRouterEmptyTablesReadIsProdDirect(t *testing.T) {
	dm := delta.NewMap()
	dm.Add("users", "42")
	ts := delta.NewTombstoneSet()
	r := NewRouter(dm, ts, nil)

	cl := &Classification{OpType: OpRead, SubType: SubSelect, Tables: nil}
	if got := r.Route(cl); got != StrategyProdDirect {
		t.Errorf("SELECT with no tables: got %s, want PROD_DIRECT", got)
	}
}

func TestRouterSchemaDiffTriggersMergedRead(t *testing.T) {
	reg := coreSchema.NewRegistry()
	reg.RecordAddColumn("users", coreSchema.Column{Name: "phone", Type: "TEXT"})

	r := NewRouter(nil, nil, reg)

	// SELECT on a table with schema diff → MergedRead.
	cl := &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"users"}}
	if got := r.Route(cl); got != StrategyMergedRead {
		t.Errorf("SELECT on schema-diff table: got %s, want MERGED_READ", got)
	}

	// SELECT on a clean table → ProdDirect.
	cl2 := &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"orders"}}
	if got := r.Route(cl2); got != StrategyProdDirect {
		t.Errorf("SELECT on clean table: got %s, want PROD_DIRECT", got)
	}

	// JOIN where one table has schema diff → JoinPatch.
	cl3 := &Classification{OpType: OpRead, SubType: SubSelect, Tables: []string{"users", "orders"}, IsJoin: true}
	if got := r.Route(cl3); got != StrategyJoinPatch {
		t.Errorf("JOIN with schema-diff table: got %s, want JOIN_PATCH", got)
	}
}
