package proxy

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/firestore/schema"
	"github.com/mori-dev/mori/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Proxy is a gRPC reverse proxy that intercepts Firestore API calls
// and routes reads to prod, writes to shadow (emulator).
// It uses SDK-based clients for merged reads and delta-tracked writes
// when available, falling back to raw byte forwarding otherwise.
type Proxy struct {
	prodAddr        string
	shadowAddr      string
	credentialsFile string
	projectID       string
	databaseID      string // Firestore database ID (default: "(default)")
	port            int
	verbose         bool

	classifier     core.Classifier
	router         *core.Router
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	collections    map[string]schema.CollectionMeta
	schemaRegistry *coreSchema.Registry
	moriDir        string
	logger         *logging.Logger

	// Raw gRPC connections (for fallback byte forwarding).
	prodConn   *grpc.ClientConn
	shadowConn *grpc.ClientConn
	grpcServer *grpc.Server

	// SDK clients for merged reads and delta-tracked writes.
	sdk *sdkClients

	listenerMu sync.Mutex
	listener   net.Listener

	connCount  atomic.Int64
	shutdownCh chan struct{}
	once       sync.Once

	// Transaction tracking: maps transaction ID (hex-encoded) to active state.
	txnMu             sync.Mutex
	activeTransactions map[string]bool
	inTransaction      bool // true if any transaction is staged (simplified: single-session)

	// txnReadTime stores the prod-side read timestamp captured at BeginTransaction.
	// All prod reads within a transaction use this timestamp for snapshot isolation,
	// matching Firestore's guarantee that all reads in a transaction see a consistent snapshot.
	txnReadTime map[string]*timestamppb.Timestamp
}

// New creates a Firestore Proxy.
func New(
	prodAddr, shadowAddr, credentialsFile, projectID, databaseID string,
	listenPort int, verbose bool,
	classifier core.Classifier, router *core.Router,
	deltaMap *delta.Map, tombstones *delta.TombstoneSet,
	collections map[string]schema.CollectionMeta, moriDir string,
	schemaRegistry *coreSchema.Registry,
	logger *logging.Logger,
) *Proxy {
	if databaseID == "" {
		databaseID = "(default)"
	}

	return &Proxy{
		prodAddr:           prodAddr,
		shadowAddr:         shadowAddr,
		credentialsFile:    credentialsFile,
		projectID:          projectID,
		databaseID:         databaseID,
		port:               listenPort,
		verbose:            verbose,
		classifier:         classifier,
		router:             router,
		deltaMap:           deltaMap,
		tombstones:         tombstones,
		collections:        collections,
		schemaRegistry:     schemaRegistry,
		moriDir:            moriDir,
		logger:             logger,
		shutdownCh:         make(chan struct{}),
		activeTransactions: make(map[string]bool),
		txnReadTime:        make(map[string]*timestamppb.Timestamp),
	}
}

// ListenAndServe establishes backend connections and starts the gRPC proxy server.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	// Connect to prod backend (raw gRPC for fallback forwarding).
	var err error
	p.prodConn, err = p.dialProd(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to prod Firestore: %w", err)
	}

	// Connect to shadow backend (emulator) for raw forwarding.
	if p.shadowAddr != "" {
		p.shadowConn, err = grpc.NewClient(p.shadowAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
		)
		if err != nil {
			return fmt.Errorf("failed to connect to shadow emulator: %w", err)
		}
	}

	// Initialize SDK clients for merged reads and delta-tracked writes.
	if p.shadowAddr != "" {
		sdkC, sdkErr := newSDKClients(ctx, p.prodAddr, p.shadowAddr, p.credentialsFile, p.projectID)
		if sdkErr != nil {
			log.Printf("Warning: failed to create SDK clients (merged reads unavailable): %v", sdkErr)
			// Non-fatal — fall back to raw forwarding.
		} else {
			p.sdk = sdkC
			log.Println("Firestore SDK clients initialized — merged reads and delta tracking enabled")
		}
	}

	// Create gRPC server with unknown service handler.
	p.grpcServer = grpc.NewServer(
		grpc.UnknownServiceHandler(p.handler),
		grpc.ForceServerCodec(rawCodec{}),
	)

	addr := fmt.Sprintf("127.0.0.1:%d", p.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	p.listenerMu.Lock()
	p.listener = ln
	p.listenerMu.Unlock()

	if p.shadowConn != nil {
		mode := "raw-proxy"
		if p.sdk != nil {
			mode = "sdk-merged"
		}
		log.Printf("Mori Firestore proxy listening on %s (gRPC, %s) → prod=%s shadow=%s",
			ln.Addr().String(), mode, p.prodAddr, p.shadowAddr)
	} else {
		log.Printf("Mori Firestore proxy listening on %s (gRPC) → %s (pass-through)",
			ln.Addr().String(), p.prodAddr)
	}

	// Shutdown watcher.
	go func() {
		select {
		case <-ctx.Done():
		case <-p.shutdownCh:
		}
		p.grpcServer.GracefulStop()
	}()

	if err := p.grpcServer.Serve(ln); err != nil {
		select {
		case <-p.shutdownCh:
			return nil
		case <-ctx.Done():
			return nil
		default:
			return err
		}
	}
	return nil
}

// Shutdown initiates graceful shutdown.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.once.Do(func() { close(p.shutdownCh) })

	if p.grpcServer != nil {
		p.grpcServer.GracefulStop()
	}
	if p.sdk != nil {
		p.sdk.close()
	}
	if p.prodConn != nil {
		p.prodConn.Close()
	}
	if p.shadowConn != nil {
		p.shadowConn.Close()
	}
	if p.logger != nil {
		p.logger.Close()
	}
	log.Println("Firestore proxy stopped.")
	return nil
}

// Addr returns the listener's address, or "" if not yet listening.
func (p *Proxy) Addr() string {
	p.listenerMu.Lock()
	ln := p.listener
	p.listenerMu.Unlock()
	if ln == nil {
		return ""
	}
	return ln.Addr().String()
}

// dialProd establishes a gRPC connection to the production Firestore backend.
func (p *Proxy) dialProd(_ context.Context) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))

	// Detect if connecting to an emulator (no TLS, no auth).
	if isLocalAddr(p.prodAddr) {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		// Production Firestore requires TLS + auth.
		creds := credentials.NewClientTLSFromCert(nil, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))

		// Add per-RPC credentials if a credentials file is provided.
		if p.credentialsFile != "" {
			perRPC, err := oauth.NewServiceAccountFromFile(p.credentialsFile,
				"https://www.googleapis.com/auth/datastore",
			)
			if err != nil {
				return nil, fmt.Errorf("failed to load credentials from %q: %w", p.credentialsFile, err)
			}
			opts = append(opts, grpc.WithPerRPCCredentials(perRPC))
		}
	}

	return grpc.NewClient(p.prodAddr, opts...)
}

// isLocalAddr checks if an address is a local/emulator address.
func isLocalAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "0.0.0.0"
}

// rawCodec is a gRPC codec that passes through raw bytes without
// protobuf marshaling/unmarshaling. This lets the proxy forward
// gRPC frames opaquely.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*frame)
	if !ok {
		return nil, status.Errorf(codes.Internal, "rawCodec: unexpected type %T", v)
	}
	return f.payload, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*frame)
	if !ok {
		return status.Errorf(codes.Internal, "rawCodec: unexpected type %T", v)
	}
	f.payload = data
	return nil
}

func (rawCodec) Name() string {
	return "raw"
}
