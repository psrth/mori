package core

import "testing"

func TestNewRouterNotNil(t *testing.T) {
	r := NewRouter()
	if r == nil {
		t.Fatal("NewRouter() returned nil")
	}
}

func TestRouterStubAlwaysReturnsProdDirect(t *testing.T) {
	r := NewRouter()

	classifications := []*Classification{
		{OpType: OpRead, SubType: SubSelect, Tables: []string{"users"}},
		{OpType: OpWrite, SubType: SubInsert, Tables: []string{"users"}},
		{OpType: OpWrite, SubType: SubUpdate, Tables: []string{"orders"}},
		{OpType: OpWrite, SubType: SubDelete, Tables: []string{"users"}},
		{OpType: OpDDL, SubType: SubAlter, Tables: []string{"users"}},
		{OpType: OpTransaction, SubType: SubBegin},
	}

	for _, c := range classifications {
		got := r.Route(c)
		if got != StrategyProdDirect {
			t.Errorf("Route(%s/%s) = %s, want PROD_DIRECT", c.OpType, c.SubType, got)
		}
	}
}
