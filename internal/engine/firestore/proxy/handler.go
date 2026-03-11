package proxy

import (
	"io"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/engine/firestore/classify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type routeTarget int

const (
	targetProd        routeTarget = iota
	targetShadow                  // raw forward to shadow
	targetMerged                  // SDK-based merged read
	targetSDKWrite                // SDK-based write with delta tracking
	targetTransaction             // SDK-based transaction control (BeginTransaction/Rollback)
)

// handler is the gRPC unknown service handler that routes Firestore calls.
func (p *Proxy) handler(srv any, serverStream grpc.ServerStream) error {
	method, ok := grpc.Method(serverStream.Context())
	if !ok {
		return status.Error(codes.Internal, "mori-firestore: could not determine gRPC method")
	}

	connID := p.connCount.Add(1)

	// Classify the method.
	cl, err := p.classifier.Classify(method)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] classify error for %s: %v", connID, method, err)
		}
		return p.forwardToProd(serverStream, method, connID)
	}

	// Extract collection names from gRPC metadata if available.
	// The request body is not yet read, so we extract from metadata headers.
	// Full collection extraction happens at the SDK handler level after deserialization.
	// For now, populate Tables if we have resource path info from metadata.
	p.populateTablesFromMetadata(serverStream, cl, method)

	// Route.
	strategy := p.router.Route(cl)

	// WRITE GUARD L1: validate routing decision.
	if err := validateRouteDecision(cl, strategy, connID, p.logger); err != nil {
		strategy = core.StrategyShadowWrite
	}

	if p.verbose {
		log.Printf("[conn %d] %s/%s → %s | %s",
			connID, cl.OpType, cl.SubType, strategy, method)
	}

	if p.logger != nil {
		p.logger.Query(connID, method, cl, strategy, 0)
	}

	// Block unsupported operations before routing.
	if strategy == core.StrategyNotSupported {
		msg := core.UnsupportedTransactionMsg
		if cl.NotSupportedMsg != "" {
			msg = cl.NotSupportedMsg
		}
		return status.Error(codes.Unimplemented, msg)
	}

	// Determine target using SDK-aware routing.
	target := p.resolveTargetSDK(cl, strategy, method)

	switch target {
	case targetMerged:
		// SDK-based merged read.
		return p.handleSDKRead(serverStream, method, connID)

	case targetSDKWrite:
		// SDK-based write with delta/tombstone tracking.
		return p.handleSDKWrite(serverStream, method, connID)

	case targetTransaction:
		// SDK-based transaction control.
		return p.handleSDKTransaction(serverStream, method, connID)

	case targetShadow:
		return p.forwardToShadow(serverStream, method, connID)

	default:
		// WRITE GUARD L2: block write methods from reaching prod.
		if err := guardProdMethod(method, connID, p.verbose, p.logger); err != nil {
			return status.Errorf(codes.PermissionDenied,
				"mori: write operation blocked — %s", err)
		}
		// WRITE GUARD L3: final check.
		if cl.OpType == core.OpWrite || cl.OpType == core.OpDDL {
			log.Printf("[CRITICAL] [conn %d] WRITE GUARD L3: %s/%s reached targetProd — BLOCKED",
				connID, cl.OpType, cl.SubType)
			return status.Error(codes.PermissionDenied,
				"mori: write operation blocked — internal routing error detected")
		}
		return p.forwardToProd(serverStream, method, connID)
	}
}

// resolveTargetSDK determines the routing target, preferring SDK-based handling
// when SDK clients are available.
func (p *Proxy) resolveTargetSDK(cl *core.Classification, strategy core.RoutingStrategy, method string) routeTarget {
	methodName := extractMethodNameFromHandler(method)

	// Use SDK-based merged reads when SDK clients are available and the method supports it.
	if p.sdk != nil && p.sdk.prod != nil && p.sdk.shadow != nil {
		// Merged reads via SDK.
		if strategy == core.StrategyMergedRead || strategy == core.StrategyJoinPatch {
			switch methodName {
			case "GetDocument", "BatchGetDocuments", "ListDocuments", "RunQuery",
				"RunAggregationQuery", "ListCollectionIds":
				return targetMerged
			}
		}

		// Reads that should go to prod but need delta/tombstone handling.
		// With collection name extraction in populateTablesFromMetadata(),
		// the router now has table names for per-collection routing.
		// However, for safety, still upgrade to merged read when any deltas
		// exist and the router couldn't determine the affected tables.
		if cl.OpType == core.OpRead && strategy == core.StrategyProdDirect {
			switch methodName {
			case "GetDocument", "BatchGetDocuments", "ListDocuments", "RunQuery",
				"RunAggregationQuery", "ListCollectionIds":
				if p.deltaMap.HasAnyDelta() || len(p.tombstones.Tables()) > 0 {
					return targetMerged
				}
			}
		}

		// Writes via SDK with delta tracking.
		if cl.OpType == core.OpWrite {
			switch methodName {
			case "CreateDocument", "UpdateDocument", "DeleteDocument",
				"Commit", "BatchWrite":
				return targetSDKWrite
			case "Write":
				// Bidirectional streaming write — handle with delta tracking.
				return targetSDKWrite
			}
		}

		// Transaction control via SDK.
		if cl.OpType == core.OpTransaction {
			switch methodName {
			case "BeginTransaction", "Rollback":
				return targetTransaction
			}
		}
	}

	// Fallback to legacy routing.
	return p.resolveTargetLegacy(cl, strategy)
}

// resolveTargetLegacy is the original routing logic for when SDK clients are
// not available (fallback mode).
func (p *Proxy) resolveTargetLegacy(cl *core.Classification, strategy core.RoutingStrategy) routeTarget {
	switch strategy {
	case core.StrategyShadowWrite,
		core.StrategyHydrateAndWrite,
		core.StrategyShadowDelete,
		core.StrategyShadowDDL:
		return targetShadow

	case core.StrategyTransaction:
		return targetShadow

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		// Without SDK clients, fall back to shadow-only reads.
		return targetShadow

	case core.StrategyNotSupported:
		return targetProd // Intercepted by handler before target dispatch.

	case core.StrategyProdDirect:
		return targetProd

	default:
		return targetProd // Unknown strategy — will be caught by handler's StrategyNotSupported check.
	}
}

// handleSDKRead deserializes the incoming gRPC request, performs a merged read
// via the SDK, and sends the result back over the server stream.
func (p *Proxy) handleSDKRead(serverStream grpc.ServerStream, method string, connID int64) error {
	ctx := serverStream.Context()
	methodName := extractMethodNameFromHandler(method)

	// Receive the request frame.
	f := &frame{}
	if err := serverStream.RecvMsg(f); err != nil {
		return err
	}

	rh := &readHandler{
		prodClient:          p.sdk.prod,
		shadowClient:        p.sdk.shadow,
		deltaMap:            p.deltaMap,
		tombstones:          p.tombstones,
		projectID:           p.projectID,
		databaseID:          p.databaseID,
		verbose:             p.verbose,
		activeTransactionFn: p.isActiveTransaction,
	}

	switch methodName {
	case "GetDocument":
		req := &firestorepb.GetDocumentRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal GetDocumentRequest: %v — falling back to raw", connID, err)
			return p.forwardToProd(serverStream, method, connID)
		}

		// If the request carries a transaction, use its read timestamp for prod-side snapshot isolation.
		if txnSel, ok := req.GetConsistencySelector().(*firestorepb.GetDocumentRequest_Transaction); ok && txnSel != nil {
			if rt := p.getTransactionReadTime(string(txnSel.Transaction)); rt != nil {
				rh.readTime = rt
			}
		}

		doc, err := rh.getDocument(ctx, req)
		if err != nil {
			return err
		}

		respBytes, err := proto.Marshal(doc)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal GetDocument response: %v", err)
		}
		return serverStream.SendMsg(&frame{payload: respBytes})

	case "ListDocuments":
		req := &firestorepb.ListDocumentsRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal ListDocumentsRequest: %v — falling back to raw", connID, err)
			return p.forwardToProd(serverStream, method, connID)
		}

		resp, err := rh.listDocuments(ctx, req)
		if err != nil {
			return err
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal ListDocuments response: %v", err)
		}
		return serverStream.SendMsg(&frame{payload: respBytes})

	case "BatchGetDocuments":
		req := &firestorepb.BatchGetDocumentsRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal BatchGetDocumentsRequest: %v — falling back to raw", connID, err)
			return p.forwardToProd(serverStream, method, connID)
		}

		if txnSel, ok := req.GetConsistencySelector().(*firestorepb.BatchGetDocumentsRequest_Transaction); ok && txnSel != nil {
			if rt := p.getTransactionReadTime(string(txnSel.Transaction)); rt != nil {
				rh.readTime = rt
			}
		}

		results, err := rh.batchGetDocuments(ctx, req)
		if err != nil {
			return err
		}

		for _, r := range results {
			respBytes, err := proto.Marshal(r)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to marshal BatchGetDocuments response: %v", err)
			}
			if err := serverStream.SendMsg(&frame{payload: respBytes}); err != nil {
				return err
			}
		}
		return nil

	case "RunQuery":
		req := &firestorepb.RunQueryRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal RunQueryRequest: %v — falling back to raw", connID, err)
			return p.forwardToProd(serverStream, method, connID)
		}

		if txnSel, ok := req.GetConsistencySelector().(*firestorepb.RunQueryRequest_Transaction); ok && txnSel != nil {
			if rt := p.getTransactionReadTime(string(txnSel.Transaction)); rt != nil {
				rh.readTime = rt
			}
		}

		results, err := rh.runQuery(ctx, req)
		if err != nil {
			return err
		}

		// Stream each result back.
		for _, r := range results {
			respBytes, err := proto.Marshal(r)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to marshal RunQuery response: %v", err)
			}
			if err := serverStream.SendMsg(&frame{payload: respBytes}); err != nil {
				return err
			}
		}
		return nil

	case "RunAggregationQuery":
		req := &firestorepb.RunAggregationQueryRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal RunAggregationQueryRequest: %v — falling back to raw", connID, err)
			return p.forwardToProd(serverStream, method, connID)
		}

		if txnSel, ok := req.GetConsistencySelector().(*firestorepb.RunAggregationQueryRequest_Transaction); ok && txnSel != nil {
			if rt := p.getTransactionReadTime(string(txnSel.Transaction)); rt != nil {
				rh.readTime = rt
			}
		}

		results, err := rh.runAggregationQuery(ctx, req)
		if err != nil {
			return err
		}

		for _, r := range results {
			respBytes, err := proto.Marshal(r)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to marshal RunAggregationQuery response: %v", err)
			}
			if err := serverStream.SendMsg(&frame{payload: respBytes}); err != nil {
				return err
			}
		}
		return nil

	case "ListCollectionIds":
		req := &firestorepb.ListCollectionIdsRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal ListCollectionIdsRequest: %v — falling back to raw", connID, err)
			return p.forwardToProd(serverStream, method, connID)
		}

		resp, err := rh.listCollectionIds(ctx, req)
		if err != nil {
			return err
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal ListCollectionIds response: %v", err)
		}
		return serverStream.SendMsg(&frame{payload: respBytes})

	default:
		// Should not reach here; fall back to prod.
		return p.forwardToProd(serverStream, method, connID)
	}
}

// handleSDKWrite deserializes the incoming gRPC write request, executes on
// shadow via SDK, tracks deltas/tombstones, and sends the result back.
func (p *Proxy) handleSDKWrite(serverStream grpc.ServerStream, method string, connID int64) error {
	ctx := serverStream.Context()
	methodName := extractMethodNameFromHandler(method)

	// WRITE GUARD: ensure we never hit prod.
	if err := guardProdMethod(method, connID, p.verbose, p.logger); err == nil {
		// guardProdMethod returns nil for non-write methods — this is a write, so double-check.
		if !classify.IsWriteMethod(method) {
			return p.forwardToProd(serverStream, method, connID)
		}
	}

	// Receive the request frame.
	f := &frame{}
	if err := serverStream.RecvMsg(f); err != nil {
		return err
	}

	var hy *hydrator
	if p.sdk.prod != nil {
		hy = &hydrator{
			prodClient:   p.sdk.prod,
			shadowClient: p.sdk.shadow,
			deltaMap:     p.deltaMap,
			tombstones:   p.tombstones,
			verbose:      p.verbose,
		}
	}

	p.txnMu.Lock()
	inTxn := p.inTransaction
	p.txnMu.Unlock()

	wh := &writeHandler{
		shadowClient:  p.sdk.shadow,
		prodClient:    p.sdk.prod,
		deltaMap:      p.deltaMap,
		tombstones:    p.tombstones,
		moriDir:       p.moriDir,
		verbose:       p.verbose,
		hydrator:      hy,
		inTransaction: inTxn,
	}

	switch methodName {
	case "CreateDocument":
		req := &firestorepb.CreateDocumentRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal CreateDocumentRequest: %v — falling back to shadow raw", connID, err)
			return p.forwardToShadow(serverStream, method, connID)
		}

		doc, err := wh.createDocument(ctx, req)
		if err != nil {
			return err
		}

		respBytes, err := proto.Marshal(doc)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal CreateDocument response: %v", err)
		}
		return serverStream.SendMsg(&frame{payload: respBytes})

	case "UpdateDocument":
		req := &firestorepb.UpdateDocumentRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal UpdateDocumentRequest: %v — falling back to shadow raw", connID, err)
			return p.forwardToShadow(serverStream, method, connID)
		}

		doc, err := wh.updateDocument(ctx, req)
		if err != nil {
			return err
		}

		respBytes, err := proto.Marshal(doc)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal UpdateDocument response: %v", err)
		}
		return serverStream.SendMsg(&frame{payload: respBytes})

	case "DeleteDocument":
		req := &firestorepb.DeleteDocumentRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal DeleteDocumentRequest: %v — falling back to shadow raw", connID, err)
			return p.forwardToShadow(serverStream, method, connID)
		}

		if err := wh.deleteDocument(ctx, req); err != nil {
			return err
		}

		// DeleteDocument returns Empty. Send an empty frame.
		return serverStream.SendMsg(&frame{payload: nil})

	case "Commit":
		req := &firestorepb.CommitRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal CommitRequest: %v — falling back to shadow raw", connID, err)
			return p.forwardToShadow(serverStream, method, connID)
		}

		resp, err := wh.commitWrite(ctx, req)
		if err != nil {
			// If commit fails and there was a transaction, roll back the staged deltas.
			if txnID := string(req.GetTransaction()); txnID != "" && p.isActiveTransaction(txnID) {
				p.txnMu.Lock()
				p.deltaMap.Rollback()
				p.deltaMap.RollbackInsertCounts()
				p.tombstones.Rollback()
				delete(p.activeTransactions, txnID)
				delete(p.txnReadTime, txnID)
				if len(p.activeTransactions) == 0 {
					p.inTransaction = false
				}
				p.txnMu.Unlock()
			}
			return err
		}

		// If this commit was part of a transaction, promote staged deltas.
		if txnID := string(req.GetTransaction()); txnID != "" {
			p.commitTransaction(txnID)
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal Commit response: %v", err)
		}
		return serverStream.SendMsg(&frame{payload: respBytes})

	case "BatchWrite":
		req := &firestorepb.BatchWriteRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal BatchWriteRequest: %v — falling back to shadow raw", connID, err)
			return p.forwardToShadow(serverStream, method, connID)
		}

		resp, err := wh.batchWrite(ctx, req)
		if err != nil {
			return err
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal BatchWrite response: %v", err)
		}
		return serverStream.SendMsg(&frame{payload: respBytes})

	case "Write":
		// The Write RPC is a bidirectional stream. Forward to shadow with delta tracking.
		// We intercept write frames, track deltas/tombstones, then forward.
		return p.handleWriteStream(serverStream, method, connID, wh)

	default:
		// Fall back to shadow raw forwarding.
		return p.forwardToShadow(serverStream, method, connID)
	}
}

// handleWriteStream handles the bidirectional Write streaming RPC.
// It forwards frames to shadow while intercepting write operations for delta tracking.
func (p *Proxy) handleWriteStream(serverStream grpc.ServerStream, method string, connID int64, wh *writeHandler) error {
	if p.shadowConn == nil {
		return status.Error(codes.Unavailable, "mori-firestore: shadow emulator unavailable")
	}

	ctx := serverStream.Context()

	// Open a stream to the shadow backend.
	desc := &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}
	clientStream, err := p.shadowConn.NewStream(ctx, desc, method)
	if err != nil {
		return err
	}

	// Forward client -> shadow in a goroutine, tracking writes.
	s2cErr := make(chan error, 1)
	go func() {
		for {
			f := &frame{}
			if err := serverStream.RecvMsg(f); err != nil {
				s2cErr <- err
				return
			}

			// Try to parse as WriteRequest for delta tracking.
			writeReq := &firestorepb.WriteRequest{}
			if parseErr := proto.Unmarshal(f.payload, writeReq); parseErr == nil {
				for _, w := range writeReq.GetWrites() {
					// Hydrate before write if needed.
					if wh.hydrator != nil {
						wh.hydrateForWrite(ctx, w)
					}
					wh.trackWrite(w)
				}
			}

			if err := clientStream.SendMsg(f); err != nil {
				s2cErr <- err
				return
			}
		}
	}()

	// Forward shadow -> client in a goroutine.
	c2sErr := make(chan error, 1)
	go func() {
		c2sErr <- forwardFrames(clientStream, serverStream)
	}()

	// Wait for either direction to finish.
	for range 2 {
		select {
		case err := <-s2cErr:
			if err == io.EOF {
				clientStream.CloseSend()
			} else if err != nil {
				return err
			}
		case err := <-c2sErr:
			if err != nil && err != io.EOF {
				return err
			}
			// Persist deltas after stream completes.
			wh.persistDelta()
			wh.persistTombstone()
			return nil
		}
	}

	wh.persistDelta()
	wh.persistTombstone()
	return nil
}

// handleSDKTransaction handles BeginTransaction and Rollback with staged delta support.
func (p *Proxy) handleSDKTransaction(serverStream grpc.ServerStream, method string, connID int64) error {
	ctx := serverStream.Context()
	methodName := extractMethodNameFromHandler(method)

	// Receive the request frame.
	f := &frame{}
	if err := serverStream.RecvMsg(f); err != nil {
		return err
	}

	switch methodName {
	case "BeginTransaction":
		req := &firestorepb.BeginTransactionRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal BeginTransactionRequest: %v — falling back to shadow raw", connID, err)
			return p.forwardToShadow(serverStream, method, connID)
		}

		// Capture the current time BEFORE forwarding to shadow. This timestamp
		// will be used as ReadTime for all prod-side reads within this transaction,
		// providing snapshot isolation equivalent to Firestore's native transactions.
		readTime := timestamppb.New(time.Now())

		// Forward to shadow.
		resp, err := p.sdk.shadow.BeginTransaction(ctx, req)
		if err != nil {
			return err
		}

		// Stage deltas and tombstones for this transaction.
		// When Stage() is called, subsequent Add() calls go to committed map directly,
		// but we track the transaction for Commit/Rollback correlation.
		txnID := string(resp.GetTransaction())
		p.txnMu.Lock()
		p.activeTransactions[txnID] = true
		p.inTransaction = true
		p.txnReadTime[txnID] = readTime
		p.txnMu.Unlock()

		if p.verbose {
			log.Printf("[conn %d] BeginTransaction: staged delta/tombstone tracking for txn", connID)
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal BeginTransaction response: %v", err)
		}
		return serverStream.SendMsg(&frame{payload: respBytes})

	case "Rollback":
		req := &firestorepb.RollbackRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal RollbackRequest: %v — falling back to shadow raw", connID, err)
			return p.forwardToShadow(serverStream, method, connID)
		}

		// Forward to shadow.
		err := p.sdk.shadow.Rollback(ctx, req)
		if err != nil {
			return err
		}

		// Rollback deltas and tombstones.
		txnID := string(req.GetTransaction())
		p.txnMu.Lock()
		if p.activeTransactions[txnID] {
			p.deltaMap.Rollback()
			p.deltaMap.RollbackInsertCounts()
			p.tombstones.Rollback()
			delete(p.activeTransactions, txnID)
			delete(p.txnReadTime, txnID)
			if len(p.activeTransactions) == 0 {
				p.inTransaction = false
			}
		}
		p.txnMu.Unlock()

		if p.verbose {
			log.Printf("[conn %d] Rollback: rolled back staged deltas/tombstones", connID)
		}

		// Rollback returns Empty.
		return serverStream.SendMsg(&frame{payload: nil})

	default:
		return p.forwardToShadow(serverStream, method, connID)
	}
}

// commitTransaction is called by the write handler when a Commit includes a transaction ID.
// It promotes staged deltas/tombstones to committed state.
func (p *Proxy) commitTransaction(txnID string) {
	p.txnMu.Lock()
	defer p.txnMu.Unlock()

	if p.activeTransactions[txnID] {
		p.deltaMap.Commit()
		p.deltaMap.CommitInsertCounts()
		p.tombstones.Commit()
		delete(p.activeTransactions, txnID)
		delete(p.txnReadTime, txnID)
		if len(p.activeTransactions) == 0 {
			p.inTransaction = false
		}
	}
}

// isActiveTransaction reports whether the given transaction ID is tracked.
func (p *Proxy) isActiveTransaction(txnID string) bool {
	p.txnMu.Lock()
	defer p.txnMu.Unlock()
	return p.activeTransactions[txnID]
}

// getTransactionReadTime returns the prod-side read timestamp for a transaction,
// or nil if the transaction has no stored read time.
func (p *Proxy) getTransactionReadTime(txnID string) *timestamppb.Timestamp {
	p.txnMu.Lock()
	defer p.txnMu.Unlock()
	return p.txnReadTime[txnID]
}

// populateTablesFromMetadata attempts to extract collection names from gRPC metadata
// headers and populates Classification.Tables. This enables per-collection routing
// instead of relying on HasAnyDelta() globally.
func (p *Proxy) populateTablesFromMetadata(serverStream grpc.ServerStream, cl *core.Classification, method string) {
	// gRPC metadata headers may contain resource-related information.
	// Since we can't peek at the request body without consuming it,
	// we use a heuristic: check if the method path contains resource info.
	// For most Firestore methods, the actual collection info is in the request body.
	// We set Tables from delta/tombstone state to allow proper routing.
	// The real per-request collection extraction happens in handleSDKRead/handleSDKWrite.

	// For read operations, if no tables are set yet, check all tables with deltas
	// so the router can make a correct decision.
	if cl.OpType == core.OpRead && len(cl.Tables) == 0 {
		tables := p.deltaMap.Tables()
		tombTables := p.tombstones.Tables()
		seen := make(map[string]bool)
		for _, t := range tables {
			if !seen[t] {
				cl.Tables = append(cl.Tables, t)
				seen[t] = true
			}
		}
		for _, t := range tombTables {
			if !seen[t] {
				cl.Tables = append(cl.Tables, t)
				seen[t] = true
			}
		}
	}
}

// extractMethodNameFromHandler extracts the method name from a fully qualified gRPC method.
func extractMethodNameFromHandler(fullMethod string) string {
	if idx := strings.LastIndex(fullMethod, "/"); idx >= 0 {
		return fullMethod[idx+1:]
	}
	return fullMethod
}

// forwardToProd streams the request/response to the production Firestore backend.
func (p *Proxy) forwardToProd(serverStream grpc.ServerStream, method string, connID int64) error {
	return p.forward(serverStream, p.prodConn, method, connID)
}

// forwardToShadow streams the request/response to the shadow emulator backend.
func (p *Proxy) forwardToShadow(serverStream grpc.ServerStream, method string, connID int64) error {
	if p.shadowConn == nil {
		return status.Error(codes.Unavailable, "mori-firestore: shadow emulator unavailable")
	}
	return p.forward(serverStream, p.shadowConn, method, connID)
}

// forward is the generic bidirectional stream forwarder. It opens a client stream
// to the backend and copies frames in both directions.
func (p *Proxy) forward(serverStream grpc.ServerStream, backendConn *grpc.ClientConn, method string, connID int64) error {
	ctx := serverStream.Context()

	// Open a stream to the backend.
	desc := &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}
	clientStream, err := backendConn.NewStream(ctx, desc, method)
	if err != nil {
		return err
	}

	// Forward client -> backend in a goroutine.
	s2cErr := make(chan error, 1)
	go func() {
		s2cErr <- forwardFrames(serverStream, clientStream)
	}()

	// Forward backend -> client in a goroutine.
	c2sErr := make(chan error, 1)
	go func() {
		c2sErr <- forwardFrames(clientStream, serverStream)
	}()

	// Wait for either direction to finish.
	for range 2 {
		select {
		case err := <-s2cErr:
			if err == io.EOF {
				// Client finished sending. Signal backend.
				clientStream.CloseSend()
			} else if err != nil {
				return err
			}
		case err := <-c2sErr:
			// Backend finished (or error). Close server stream by returning.
			if err != nil && err != io.EOF {
				return err
			}
			return nil
		}
	}
	return nil
}

// frame is an opaque protobuf-encoded gRPC message frame.
type frame struct {
	payload []byte
}

// streamSender can send gRPC message frames.
type streamSender interface {
	SendMsg(any) error
}

// streamReceiver can receive gRPC message frames.
type streamReceiver interface {
	RecvMsg(any) error
}

// forwardFrames reads frames from src and writes them to dst.
func forwardFrames(src streamReceiver, dst streamSender) error {
	for {
		f := &frame{}
		if err := src.RecvMsg(f); err != nil {
			return err
		}
		if err := dst.SendMsg(f); err != nil {
			return err
		}
	}
}
