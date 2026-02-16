package proxy

import (
	"io"
	"log"
	"strings"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/firestore/classify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type routeTarget int

const (
	targetProd    routeTarget = iota
	targetShadow              // raw forward to shadow
	targetMerged              // SDK-based merged read
	targetSDKWrite            // SDK-based write with delta tracking
)

// handler is the gRPC unknown service handler that routes Firestore calls.
func (p *Proxy) handler(srv interface{}, serverStream grpc.ServerStream) error {
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

	// Determine target using SDK-aware routing.
	target := p.resolveTargetSDK(cl, strategy, method)

	switch target {
	case targetMerged:
		// SDK-based merged read.
		return p.handleSDKRead(serverStream, method, connID)

	case targetSDKWrite:
		// SDK-based write with delta/tombstone tracking.
		return p.handleSDKWrite(serverStream, method, connID)

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
			case "GetDocument", "ListDocuments", "RunQuery":
				return targetMerged
			}
		}

		// Reads that should go to prod but need tombstone filtering.
		if cl.OpType == core.OpRead && strategy == core.StrategyProdDirect {
			switch methodName {
			case "GetDocument", "ListDocuments", "RunQuery":
				// If there are any tombstones, use SDK to filter them.
				if p.tombstones.CountForTable("") > 0 || len(p.tombstones.Tables()) > 0 {
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

	default:
		return targetProd
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
		prodClient:   p.sdk.prod,
		shadowClient: p.sdk.shadow,
		deltaMap:     p.deltaMap,
		tombstones:   p.tombstones,
		projectID:    p.projectID,
		databaseID:   "(default)",
		verbose:      p.verbose,
	}

	switch methodName {
	case "GetDocument":
		req := &firestorepb.GetDocumentRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal GetDocumentRequest: %v — falling back to raw", connID, err)
			return p.forwardToProd(serverStream, method, connID)
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

	case "RunQuery":
		req := &firestorepb.RunQueryRequest{}
		if err := proto.Unmarshal(f.payload, req); err != nil {
			log.Printf("[conn %d] failed to unmarshal RunQueryRequest: %v — falling back to raw", connID, err)
			return p.forwardToProd(serverStream, method, connID)
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

	wh := &writeHandler{
		shadowClient: p.sdk.shadow,
		deltaMap:     p.deltaMap,
		tombstones:   p.tombstones,
		moriDir:      p.moriDir,
		verbose:      p.verbose,
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
			return err
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

	default:
		// Fall back to shadow raw forwarding.
		return p.forwardToShadow(serverStream, method, connID)
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
	for i := 0; i < 2; i++ {
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

// forwardFrames reads frames from src and writes them to dst.
func forwardFrames(src grpc.Stream, dst grpc.Stream) error {
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
