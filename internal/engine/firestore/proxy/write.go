package proxy

import (
	"context"
	"log"

	firestore "cloud.google.com/go/firestore/apiv1"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/mori-dev/mori/internal/core/delta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// writeHandler executes write operations on the shadow emulator and tracks
// deltas/tombstones for merged reads.
type writeHandler struct {
	shadowClient  *firestore.Client
	prodClient    *firestore.Client // for hydration
	deltaMap      *delta.Map
	tombstones    *delta.TombstoneSet
	moriDir       string
	verbose       bool
	hydrator      *hydrator // document hydration before writes
	inTransaction bool      // if true, use Stage() instead of Add()
}

// createDocument forwards CreateDocument to shadow and tracks the delta.
func (h *writeHandler) createDocument(ctx context.Context, req *firestorepb.CreateDocumentRequest) (*firestorepb.Document, error) {
	doc, err := h.shadowClient.CreateDocument(ctx, req)
	if err != nil {
		return nil, err
	}

	// Track delta.
	collection, fullDocKey := splitDocPathFull(doc.GetName())
	if collection != "" && fullDocKey != "" {
		if h.inTransaction {
			h.deltaMap.Stage(collection, fullDocKey)
			h.deltaMap.StageInsertCount(collection, 1)
		} else {
			h.deltaMap.Add(collection, fullDocKey)
			h.deltaMap.MarkInserted(collection)
		}
		h.persistDelta()
		if h.verbose {
			log.Printf("[firestore-write] CreateDocument: tracked delta %s/%s (txn=%v)", collection, fullDocKey, h.inTransaction)
		}
	}

	return doc, nil
}

// updateDocument forwards UpdateDocument to shadow and tracks the delta.
// Hydrates the document from prod first if not already in shadow.
// Evaluates preconditions against the correct backend.
func (h *writeHandler) updateDocument(ctx context.Context, req *firestorepb.UpdateDocumentRequest) (*firestorepb.Document, error) {
	docPath := ""
	if req.GetDocument() != nil {
		docPath = req.GetDocument().GetName()
	}

	// Evaluate preconditions against the correct backend.
	if docPath != "" && req.GetCurrentDocument() != nil {
		if err := h.evaluatePrecondition(ctx, docPath, req.GetCurrentDocument()); err != nil {
			return nil, err
		}
	}

	// Hydrate: ensure the document exists in shadow before updating.
	if h.hydrator != nil && docPath != "" {
		if err := h.hydrator.hydrateDocumentFromPath(ctx, docPath); err != nil {
			log.Printf("[firestore-write] hydration failed for UpdateDocument %s: %v", docPath, err)
			// Continue — shadow may still have the document from seeding.
		}
	}

	doc, err := h.shadowClient.UpdateDocument(ctx, req)
	if err != nil {
		return nil, err
	}

	// Track delta.
	collection, fullDocKey := splitDocPathFull(doc.GetName())
	if collection != "" && fullDocKey != "" {
		if h.inTransaction {
			h.deltaMap.Stage(collection, fullDocKey)
		} else {
			h.deltaMap.Add(collection, fullDocKey)
		}
		h.persistDelta()
		if h.verbose {
			log.Printf("[firestore-write] UpdateDocument: tracked delta %s/%s (txn=%v)", collection, fullDocKey, h.inTransaction)
		}
	}

	return doc, nil
}

// deleteDocument forwards DeleteDocument to shadow and tracks the tombstone.
// Hydrates the document from prod first if needed for precondition checks.
// Evaluates preconditions against the correct backend.
func (h *writeHandler) deleteDocument(ctx context.Context, req *firestorepb.DeleteDocumentRequest) error {
	// Evaluate preconditions against the correct backend.
	if req.GetCurrentDocument() != nil {
		if err := h.evaluatePrecondition(ctx, req.GetName(), req.GetCurrentDocument()); err != nil {
			return err
		}
	}

	// Hydrate: ensure the document exists in shadow for the delete operation.
	if h.hydrator != nil {
		if err := h.hydrator.hydrateDocumentFromPath(ctx, req.GetName()); err != nil {
			log.Printf("[firestore-write] hydration failed for DeleteDocument %s: %v", req.GetName(), err)
		}
	}

	err := h.shadowClient.DeleteDocument(ctx, req)
	if err != nil {
		return err
	}

	// Track tombstone.
	collection, fullDocKey := splitDocPathFull(req.GetName())
	if collection != "" && fullDocKey != "" {
		if h.inTransaction {
			h.tombstones.Stage(collection, fullDocKey)
		} else {
			h.tombstones.Add(collection, fullDocKey)
		}
		// Remove from delta map if it was there — it's now tombstoned.
		h.deltaMap.Remove(collection, fullDocKey)
		h.persistTombstone()
		h.persistDelta()
		if h.verbose {
			log.Printf("[firestore-write] DeleteDocument: tracked tombstone %s/%s (txn=%v)", collection, fullDocKey, h.inTransaction)
		}
	}

	return nil
}

// commitWrite handles Commit requests by forwarding to shadow and tracking
// all document mutations in the commit.
func (h *writeHandler) commitWrite(ctx context.Context, req *firestorepb.CommitRequest) (*firestorepb.CommitResponse, error) {
	// Hydrate documents before forwarding to shadow.
	if h.hydrator != nil {
		for _, w := range req.GetWrites() {
			h.hydrateForWrite(ctx, w)
		}
	}

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
	// Hydrate documents before forwarding to shadow.
	if h.hydrator != nil {
		for _, w := range req.GetWrites() {
			h.hydrateForWrite(ctx, w)
		}
	}

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
// delta/tombstone tracking accordingly. If inTransaction is true, uses
// Stage() instead of Add() so that entries can be rolled back.
func (h *writeHandler) trackWrite(w *firestorepb.Write) {
	switch op := w.GetOperation().(type) {
	case *firestorepb.Write_Update:
		if op.Update != nil {
			collection, fullDocKey := splitDocPathFull(op.Update.GetName())
			if collection != "" && fullDocKey != "" {
				if h.inTransaction {
					h.deltaMap.Stage(collection, fullDocKey)
				} else {
					h.deltaMap.Add(collection, fullDocKey)
				}
				// Detect new document creation: Write_Update with Precondition{Exists: false}
				// indicates a create via Commit/BatchWrite. Track insert count so that
				// HasInserts() returns true and merged reads include the shadow query path.
				if precond := w.GetCurrentDocument(); precond != nil {
					if existsCond, ok := precond.GetConditionType().(*firestorepb.Precondition_Exists); ok && !existsCond.Exists {
						if h.inTransaction {
							h.deltaMap.StageInsertCount(collection, 1)
						} else {
							h.deltaMap.MarkInserted(collection)
						}
					}
				}
				if h.verbose {
					log.Printf("[firestore-write] Commit/BatchWrite: tracked delta %s/%s (txn=%v)", collection, fullDocKey, h.inTransaction)
				}
			}
		}
	case *firestorepb.Write_Delete:
		collection, fullDocKey := splitDocPathFull(op.Delete)
		if collection != "" && fullDocKey != "" {
			if h.inTransaction {
				h.tombstones.Stage(collection, fullDocKey)
			} else {
				h.tombstones.Add(collection, fullDocKey)
			}
			h.deltaMap.Remove(collection, fullDocKey)
			if h.verbose {
				log.Printf("[firestore-write] Commit/BatchWrite: tracked tombstone %s/%s (txn=%v)", collection, fullDocKey, h.inTransaction)
			}
		}
	case *firestorepb.Write_Transform:
		if op.Transform != nil {
			collection, fullDocKey := splitDocPathFull(op.Transform.GetDocument())
			if collection != "" && fullDocKey != "" {
				if h.inTransaction {
					h.deltaMap.Stage(collection, fullDocKey)
				} else {
					h.deltaMap.Add(collection, fullDocKey)
				}
				if h.verbose {
					log.Printf("[firestore-write] Commit/BatchWrite: tracked delta (transform) %s/%s (txn=%v)", collection, fullDocKey, h.inTransaction)
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

// evaluatePrecondition checks document preconditions against the correct backend.
// If the document is in the delta map, check shadow. If tombstoned, treat as not existing.
// Otherwise, check prod.
func (h *writeHandler) evaluatePrecondition(ctx context.Context, docPath string, precondition *firestorepb.Precondition) error {
	if precondition == nil {
		return nil
	}

	collection, fullDocKey := splitDocPathFull(docPath)
	if collection == "" || fullDocKey == "" {
		return nil
	}

	switch cond := precondition.GetConditionType().(type) {
	case *firestorepb.Precondition_Exists:
		exists := h.documentExists(ctx, collection, fullDocKey, docPath)
		if cond.Exists && !exists {
			return status.Errorf(codes.FailedPrecondition,
				"document %q does not exist (precondition: exists=true)", docPath)
		}
		if !cond.Exists && exists {
			return status.Errorf(codes.AlreadyExists,
				"document %q already exists (precondition: exists=false)", docPath)
		}

	case *firestorepb.Precondition_UpdateTime:
		// Check update_time against the correct backend.
		if h.tombstones.IsTombstoned(collection, fullDocKey) {
			return status.Errorf(codes.FailedPrecondition,
				"document %q has been deleted (precondition: update_time)", docPath)
		}
		// For update_time preconditions, we let the shadow handle it since it's complex.
		// The hydration step ensures the document state is correct in shadow.
	}

	return nil
}

// documentExists checks if a document exists by consulting the correct backend.
func (h *writeHandler) documentExists(ctx context.Context, collection, fullDocKey, docPath string) bool {
	// Tombstoned = intentionally deleted.
	if h.tombstones.IsTombstoned(collection, fullDocKey) {
		return false
	}

	// In delta map = exists in shadow.
	if h.deltaMap.IsDelta(collection, fullDocKey) {
		getReq := &firestorepb.GetDocumentRequest{Name: docPath}
		_, err := h.shadowClient.GetDocument(ctx, getReq)
		return err == nil
	}

	// Not tracked — check prod.
	if h.prodClient != nil {
		getReq := &firestorepb.GetDocumentRequest{Name: docPath}
		_, err := h.prodClient.GetDocument(ctx, getReq)
		return err == nil
	}

	return false
}

// hydrateForWrite hydrates documents needed by a Write proto before execution.
func (h *writeHandler) hydrateForWrite(ctx context.Context, w *firestorepb.Write) {
	if h.hydrator == nil {
		return
	}

	switch op := w.GetOperation().(type) {
	case *firestorepb.Write_Update:
		if op.Update != nil {
			if err := h.hydrator.hydrateDocumentFromPath(ctx, op.Update.GetName()); err != nil {
				log.Printf("[firestore-write] hydration failed for Commit/BatchWrite update %s: %v", op.Update.GetName(), err)
			}
		}
	case *firestorepb.Write_Delete:
		if err := h.hydrator.hydrateDocumentFromPath(ctx, op.Delete); err != nil {
			log.Printf("[firestore-write] hydration failed for Commit/BatchWrite delete %s: %v", op.Delete, err)
		}
	case *firestorepb.Write_Transform:
		if op.Transform != nil {
			if err := h.hydrator.hydrateDocumentFromPath(ctx, op.Transform.GetDocument()); err != nil {
				log.Printf("[firestore-write] hydration failed for Commit/BatchWrite transform %s: %v", op.Transform.GetDocument(), err)
			}
		}
	}
}

