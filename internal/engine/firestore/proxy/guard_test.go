package proxy

import (
	"testing"

	"github.com/mori-dev/mori/internal/core"
)

func TestValidateRouteDecision(t *testing.T) {
	tests := []struct {
		name     string
		cl       *core.Classification
		strategy core.RoutingStrategy
		wantErr  bool
	}{
		{
			name:     "nil classification",
			cl:       nil,
			strategy: core.StrategyProdDirect,
			wantErr:  false,
		},
		{
			name:     "read to prod is ok",
			cl:       &core.Classification{OpType: core.OpRead, SubType: core.SubSelect},
			strategy: core.StrategyProdDirect,
			wantErr:  false,
		},
		{
			name:     "write to shadow is ok",
			cl:       &core.Classification{OpType: core.OpWrite, SubType: core.SubInsert},
			strategy: core.StrategyShadowWrite,
			wantErr:  false,
		},
		{
			name:     "write to prod is blocked",
			cl:       &core.Classification{OpType: core.OpWrite, SubType: core.SubInsert},
			strategy: core.StrategyProdDirect,
			wantErr:  true,
		},
		{
			name:     "DDL to prod is blocked",
			cl:       &core.Classification{OpType: core.OpDDL, SubType: core.SubCreate},
			strategy: core.StrategyProdDirect,
			wantErr:  true,
		},
		{
			name:     "transaction to both is ok",
			cl:       &core.Classification{OpType: core.OpTransaction, SubType: core.SubBegin},
			strategy: core.StrategyProdDirect,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRouteDecision(tt.cl, tt.strategy, 1, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRouteDecision() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGuardProdMethod(t *testing.T) {
	writes := []string{
		"/google.firestore.v1.Firestore/CreateDocument",
		"/google.firestore.v1.Firestore/UpdateDocument",
		"/google.firestore.v1.Firestore/DeleteDocument",
		"/google.firestore.v1.Firestore/Commit",
		"/google.firestore.v1.Firestore/Write",
		"/google.firestore.v1.Firestore/BatchWrite",
	}
	for _, m := range writes {
		if err := guardProdMethod(m, 1, false, nil); err == nil {
			t.Errorf("guardProdMethod(%q) = nil, want error", m)
		}
	}

	reads := []string{
		"/google.firestore.v1.Firestore/GetDocument",
		"/google.firestore.v1.Firestore/ListDocuments",
		"/google.firestore.v1.Firestore/RunQuery",
		"/google.firestore.v1.Firestore/BeginTransaction",
		"/google.firestore.v1.Firestore/Rollback",
	}
	for _, m := range reads {
		if err := guardProdMethod(m, 1, false, nil); err != nil {
			t.Errorf("guardProdMethod(%q) = %v, want nil", m, err)
		}
	}
}
