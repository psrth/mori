package proxy

import (
	"context"
	"log"
	"strings"

	firestore "cloud.google.com/go/firestore/apiv1"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/mori-dev/mori/internal/core/delta"
)

// writeHandler executes write operations on the shadow emulator and tracks
// deltas/tombstones for merged reads.
type writeHandler struct {
	shadowClient *firestore.Client
	deltaMap     *delta.Map
	tombstones   *delta.TombstoneSet
	moriDir      string
	verbose      bool
}

// createDocument forwards CreateDocument to shadow and tracks the delta.
func (h *writeHandler) createDocument(ctx context.Context, req *firestorepb.CreateDocumentRequest) (*firestorepb.Document, error) {
	doc, err := h.shadowClient.CreateDocument(ctx, req)
	if err != nil {
		return nil, err
	}

	// Track delta.
	collection, docID := splitDocPath(doc.GetName())
	if collection != "" && docID != "" {
		h.deltaMap.Add(collection, docID)
		h.deltaMap.MarkInserted(collection)
		h.persistDelta()
		if h.verbose {
			log.Printf("[firestore-write] CreateDocument: tracked delta %s/%s", collection, docID)
		}
	}

	return doc, nil
}

// updateDocument forwards UpdateDocument to shadow and tracks the delta.
func (h *writeHandler) updateDocument(ctx context.Context, req *firestorepb.UpdateDocumentRequest) (*firestorepb.Document, error) {
	doc, err := h.shadowClient.UpdateDocument(ctx, req)
	if err != nil {
		return nil, err
	}

	// Track delta.
	collection, docID := splitDocPath(doc.GetName())
	if collection != "" && docID != "" {
		h.deltaMap.Add(collection, docID)
		h.persistDelta()
		if h.verbose {
			log.Printf("[firestore-write] UpdateDocument: tracked delta %s/%s", collection, docID)
		}
	}

	return doc, nil
}

// deleteDocument forwards DeleteDocument to shadow and tracks the tombstone.
func (h *writeHandler) deleteDocument(ctx context.Context, req *firestorepb.DeleteDocumentRequest) error {
	err := h.shadowClient.DeleteDocument(ctx, req)
	if err != nil {
		return err
	}

	// Track tombstone.
	collection, docID := splitDocPath(req.GetName())
	if collection != "" && docID != "" {
		h.tombstones.Add(collection, docID)
		// Remove from delta map if it was there — it's now tombstoned.
		h.deltaMap.Remove(collection, docID)
		h.persistTombstone()
		h.persistDelta()
		if h.verbose {
			log.Printf("[firestore-write] DeleteDocument: tracked tombstone %s/%s", collection, docID)
		}
	}

	return nil
}

// commitWrite handles Commit requests by forwarding to shadow and tracking
// all document mutations in the commit.
func (h *writeHandler) commitWrite(ctx context.Context, req *firestorepb.CommitRequest) (*firestorepb.CommitResponse, error) {
	resp, err := h.shadowClient.Commit(ctx, req)
	if err != nil {
		return nil, err
	}

	// Track deltas/tombstones for each write in the commit.
	for _, w := range req.GetWrites() {
		h.trackWrite(w)
	}
	h.persistDelta()
	h.persistTombstone()

	return resp, nil
}

// batchWrite handles BatchWrite requests.
func (h *writeHandler) batchWrite(ctx context.Context, req *firestorepb.BatchWriteRequest) (*firestorepb.BatchWriteResponse, error) {
	resp, err := h.shadowClient.BatchWrite(ctx, req)
	if err != nil {
		return nil, err
	}

	// Track deltas/tombstones for each write.
	for _, w := range req.GetWrites() {
		h.trackWrite(w)
	}
	h.persistDelta()
	h.persistTombstone()

	return resp, nil
}

// trackWrite extracts the document path from a Write proto and updates
// delta/tombstone tracking accordingly.
func (h *writeHandler) trackWrite(w *firestorepb.Write) {
	switch op := w.GetOperation().(type) {
	case *firestorepb.Write_Update:
		if op.Update != nil {
			collection, docID := splitDocPath(op.Update.GetName())
			if collection != "" && docID != "" {
				h.deltaMap.Add(collection, docID)
				if h.verbose {
					log.Printf("[firestore-write] Commit/BatchWrite: tracked delta %s/%s", collection, docID)
				}
			}
		}
	case *firestorepb.Write_Delete:
		collection, docID := splitDocPath(op.Delete)
		if collection != "" && docID != "" {
			h.tombstones.Add(collection, docID)
			h.deltaMap.Remove(collection, docID)
			if h.verbose {
				log.Printf("[firestore-write] Commit/BatchWrite: tracked tombstone %s/%s", collection, docID)
			}
		}
	case *firestorepb.Write_Transform:
		if op.Transform != nil {
			collection, docID := splitDocPath(op.Transform.GetDocument())
			if collection != "" && docID != "" {
				h.deltaMap.Add(collection, docID)
				if h.verbose {
					log.Printf("[firestore-write] Commit/BatchWrite: tracked delta (transform) %s/%s", collection, docID)
				}
			}
		}
	}
}

// persistDelta writes the delta map to disk.
func (h *writeHandler) persistDelta() {
	if h.moriDir == "" {
		return
	}
	if err := delta.WriteDeltaMap(h.moriDir, h.deltaMap); err != nil {
		log.Printf("[firestore-write] failed to persist delta map: %v", err)
	}
}

// persistTombstone writes the tombstone set to disk.
func (h *writeHandler) persistTombstone() {
	if h.moriDir == "" {
		return
	}
	if err := delta.WriteTombstoneSet(h.moriDir, h.tombstones); err != nil {
		log.Printf("[firestore-write] failed to persist tombstone set: %v", err)
	}
}

// extractDocPathFromCommit is a helper to get doc paths from commit writes.
// It returns collection and docID for tracking purposes.
func extractDocPathFromCommit(name string) (collection, docID string) {
	const marker = "/documents/"
	idx := strings.Index(name, marker)
	if idx < 0 {
		return "", ""
	}
	rest := name[idx+len(marker):]
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return rest, ""
	}
	return parts[len(parts)-2], parts[len(parts)-1]
}
