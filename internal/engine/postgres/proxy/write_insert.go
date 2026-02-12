package proxy

import "net"

// handleInsert executes an INSERT on Shadow and relays the response to the client.
// No delta map update is needed — locally inserted rows are identified by their
// PK being in the Shadow's sequence offset range (above Prod's max).
func (w *WriteHandler) handleInsert(clientConn net.Conn, rawMsg []byte) error {
	return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
}
