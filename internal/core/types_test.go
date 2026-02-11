package core

import "testing"

func TestOpTypeString(t *testing.T) {
	tests := []struct {
		op   OpType
		want string
	}{
		{OpRead, "READ"},
		{OpWrite, "WRITE"},
		{OpDDL, "DDL"},
		{OpTransaction, "TRANSACTION"},
		{OpOther, "OTHER"},
		{OpType(99), "OpType(99)"},
	}
	for _, tt := range tests {
		if got := tt.op.String(); got != tt.want {
			t.Errorf("OpType(%d).String() = %q, want %q", int(tt.op), got, tt.want)
		}
	}
}

func TestSubTypeString(t *testing.T) {
	tests := []struct {
		sub  SubType
		want string
	}{
		{SubSelect, "SELECT"},
		{SubInsert, "INSERT"},
		{SubUpdate, "UPDATE"},
		{SubDelete, "DELETE"},
		{SubAlter, "ALTER"},
		{SubCreate, "CREATE"},
		{SubDrop, "DROP"},
		{SubBegin, "BEGIN"},
		{SubCommit, "COMMIT"},
		{SubRollback, "ROLLBACK"},
		{SubOther, "OTHER"},
		{SubType(99), "SubType(99)"},
	}
	for _, tt := range tests {
		if got := tt.sub.String(); got != tt.want {
			t.Errorf("SubType(%d).String() = %q, want %q", int(tt.sub), got, tt.want)
		}
	}
}

func TestRoutingStrategyString(t *testing.T) {
	tests := []struct {
		s    RoutingStrategy
		want string
	}{
		{StrategyProdDirect, "PROD_DIRECT"},
		{StrategyMergedRead, "MERGED_READ"},
		{StrategyJoinPatch, "JOIN_PATCH"},
		{StrategyShadowWrite, "SHADOW_WRITE"},
		{StrategyHydrateAndWrite, "HYDRATE_AND_WRITE"},
		{StrategyShadowDelete, "SHADOW_DELETE"},
		{StrategyShadowDDL, "SHADOW_DDL"},
		{StrategyTransaction, "TRANSACTION"},
		{RoutingStrategy(99), "RoutingStrategy(99)"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("RoutingStrategy(%d).String() = %q, want %q", int(tt.s), got, tt.want)
		}
	}
}
