package proxy

import (
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
)

// handleDelete handles DELETE statements by executing on Shadow and adding tombstones.
//
// Point delete (PKs extractable): forward DELETE to Shadow, then add tombstones
// for affected PKs so they are filtered out of future merged reads.
//
// Bulk delete (no PKs): forward to Shadow without tombstoning.
func (w *WriteHandler) handleDelete(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Forward DELETE to Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	if len(cl.PKs) == 0 {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE without PKs, no tombstones added", w.connID)
		}
		return nil
	}

	// Add tombstones; conditionally remove from delta map.
	for _, pk := range cl.PKs {
		if w.inTxn() {
			w.tombstones.Stage(pk.Table, pk.PK)
			// Skip deltaMap.Remove() in txn: staged tombstone handles
			// filtering via has() which checks both committed and staged.
		} else {
			w.tombstones.Add(pk.Table, pk.PK)
			w.deltaMap.Remove(pk.Table, pk.PK)
		}
	}

	// Only persist immediately in autocommit mode; txn commit handles persistence.
	if !w.inTxn() {
		if err := delta.WriteTombstoneSet(w.moriDir, w.tombstones); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist tombstone set: %v", w.connID, err)
			}
		}
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}
