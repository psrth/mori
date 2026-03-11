package proxy

import (
	"net"
	"testing"
	"time"

	"github.com/psrth/mori/internal/core"
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
			name:     "read to prod — allowed",
			cl:       &core.Classification{OpType: core.OpRead},
			strategy: core.StrategyProdDirect,
			wantErr:  false,
		},
		{
			name:     "write to shadow — allowed",
			cl:       &core.Classification{OpType: core.OpWrite},
			strategy: core.StrategyShadowWrite,
			wantErr:  false,
		},
		{
			name:     "write to prod — BLOCKED",
			cl:       &core.Classification{OpType: core.OpWrite, SubType: core.SubInsert},
			strategy: core.StrategyProdDirect,
			wantErr:  true,
		},
		{
			name:     "DDL to prod — BLOCKED",
			cl:       &core.Classification{OpType: core.OpDDL},
			strategy: core.StrategyProdDirect,
			wantErr:  true,
		},
		{
			name:     "other to prod — allowed",
			cl:       &core.Classification{OpType: core.OpOther},
			strategy: core.StrategyProdDirect,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRouteDecision(tt.cl, tt.strategy, 1, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRouteDecision() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractCommandFromRESP(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "SET command",
			data: BuildCommandArray("SET", "key", "value").Bytes(),
			want: "SET",
		},
		{
			name: "GET command",
			data: BuildCommandArray("GET", "key").Bytes(),
			want: "GET",
		},
		{
			name: "DEL command",
			data: BuildCommandArray("DEL", "key1", "key2").Bytes(),
			want: "DEL",
		},
		{
			name: "PING",
			data: BuildCommandArray("PING").Bytes(),
			want: "PING",
		},
		{
			name: "empty",
			data: []byte{},
			want: "",
		},
		{
			name: "not RESP",
			data: []byte("hello"),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractCommandFromRESP(tt.data)
			if got != tt.want {
				t.Errorf("extractCommandFromRESP() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSafeProdConnBlocksWrites(t *testing.T) {
	inner := &fakeConn{}
	safe := NewSafeProdConn(inner, 1, false, nil)

	// Write a SET command — should be blocked.
	setCmd := BuildCommandArray("SET", "key", "value").Bytes()
	_, err := safe.Write(setCmd)
	if err == nil {
		t.Error("expected error for SET command, got nil")
	}

	// Write a GET command — should be allowed.
	getCmd := BuildCommandArray("GET", "key").Bytes()
	_, err = safe.Write(getCmd)
	if err != nil {
		t.Errorf("unexpected error for GET command: %v", err)
	}

	// Write a DEL command — should be blocked.
	delCmd := BuildCommandArray("DEL", "key").Bytes()
	_, err = safe.Write(delCmd)
	if err == nil {
		t.Error("expected error for DEL command, got nil")
	}

	// Write a PING command — should be allowed.
	pingCmd := BuildCommandArray("PING").Bytes()
	_, err = safe.Write(pingCmd)
	if err != nil {
		t.Errorf("unexpected error for PING command: %v", err)
	}
}

// fakeConn is a minimal net.Conn implementation for testing.
type fakeConn struct{}

func (f *fakeConn) Read(b []byte) (int, error)           { return 0, nil }
func (f *fakeConn) Write(b []byte) (int, error)           { return len(b), nil }
func (f *fakeConn) Close() error                           { return nil }
func (f *fakeConn) LocalAddr() net.Addr                    { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }
func (f *fakeConn) RemoteAddr() net.Addr                   { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }
func (f *fakeConn) SetDeadline(t time.Time) error          { return nil }
func (f *fakeConn) SetReadDeadline(t time.Time) error      { return nil }
func (f *fakeConn) SetWriteDeadline(t time.Time) error     { return nil }
