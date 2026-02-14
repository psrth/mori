package proxy

import (
	"io"
	"log"

	"github.com/mori-dev/mori/internal/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type routeTarget int

const (
	targetProd   routeTarget = iota
	targetShadow
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

	// Determine target.
	target := p.resolveTarget(cl, strategy)

	switch target {
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

// resolveTarget determines which backend to forward a call to.
func (p *Proxy) resolveTarget(cl *core.Classification, strategy core.RoutingStrategy) routeTarget {
	switch strategy {
	case core.StrategyShadowWrite,
		core.StrategyHydrateAndWrite,
		core.StrategyShadowDelete,
		core.StrategyShadowDDL:
		return targetShadow

	case core.StrategyTransaction:
		// Route transactions to shadow to keep write isolation.
		return targetShadow

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		// For v1, route affected reads to shadow only.
		// Shadow is seeded from prod, so it has a complete data view.
		return targetShadow

	default:
		return targetProd
	}
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
