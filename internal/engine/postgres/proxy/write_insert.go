package proxy

import (
	"net"

	"github.com/mori-dev/mori/internal/core"
)

// handleInsert executes an INSERT on Shadow and relays the response to the client.
// Marks the target table as having inserts so the Router triggers merged reads.
func (w *WriteHandler) handleInsert(clientConn net.Conn, rawMsg []byte, cl *core.Classification) error {
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}
	for _, table := range cl.Tables {
		w.deltaMap.MarkInserted(table)
	}
	return nil
}
