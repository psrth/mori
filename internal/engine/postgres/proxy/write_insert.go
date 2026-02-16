package proxy

import (
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
)

// handleInsert executes an INSERT on Shadow and relays the response to the client.
// Captures the CommandComplete tag to extract the insert count, then tracks it.
func (w *WriteHandler) handleInsert(clientConn net.Conn, rawMsg []byte, cl *core.Classification) error {
	tag, err := forwardRelayAndCaptureTag(rawMsg, w.shadowConn, clientConn)
	if err != nil {
		return err
	}
	count := parseInsertCount(tag)
	for _, table := range cl.Tables {
		if w.inTxn() {
			w.deltaMap.StageInsertCount(table, count)
		} else {
			w.deltaMap.AddInsertCount(table, count)
		}
	}

	// Persist immediately in autocommit mode; txn commit handles persistence otherwise.
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}
	return nil
}
