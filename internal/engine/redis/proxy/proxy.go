package proxy

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/redis/classify"
	"github.com/mori-dev/mori/internal/engine/redis/schema"
	"github.com/mori-dev/mori/internal/logging"
)

type routeTarget int

const (
	targetProd   routeTarget = iota
	targetShadow
	targetBoth
)

type routeDecision struct {
	target         routeTarget
	classification *core.Classification
	strategy       core.RoutingStrategy
}

// Proxy is a RESP protocol proxy that routes Redis commands to prod and shadow.
type Proxy struct {
	prodAddr   string
	shadowAddr string
	prodPass   string // password for prod Redis
	prodDB     int    // db number for prod Redis
	port       int
	verbose    bool

	classifier     core.Classifier
	router         *core.Router
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.KeyMeta
	schemaRegistry *coreSchema.Registry
	moriDir        string
	logger         *logging.Logger

	listenerMu sync.Mutex
	listener   net.Listener

	activeConns sync.WaitGroup
	connCount   atomic.Int64
	shutdownCh  chan struct{}
	once        sync.Once
}

// New creates a Redis Proxy.
func New(
	prodAddr, shadowAddr string,
	prodPass string, prodDB int,
	listenPort int, verbose bool,
	classifier core.Classifier, router *core.Router,
	deltaMap *delta.Map, tombstones *delta.TombstoneSet,
	tables map[string]schema.KeyMeta, moriDir string,
	schemaRegistry *coreSchema.Registry,
	logger *logging.Logger,
) *Proxy {
	return &Proxy{
		prodAddr:       prodAddr,
		shadowAddr:     shadowAddr,
		prodPass:       prodPass,
		prodDB:         prodDB,
		port:           listenPort,
		verbose:        verbose,
		classifier:     classifier,
		router:         router,
		deltaMap:       deltaMap,
		tombstones:     tombstones,
		tables:         tables,
		schemaRegistry: schemaRegistry,
		moriDir:        moriDir,
		logger:         logger,
		shutdownCh:     make(chan struct{}),
	}
}

// ListenAndServe binds the TCP listener and enters the accept loop.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	addr := fmt.Sprintf("127.0.0.1:%d", p.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	p.listenerMu.Lock()
	p.listener = ln
	p.listenerMu.Unlock()

	log.Printf("Mori Redis proxy listening on %s (RESP) → prod=%s shadow=%s",
		ln.Addr().String(), p.prodAddr, p.shadowAddr)

	go func() {
		select {
		case <-ctx.Done():
		case <-p.shutdownCh:
		}
		ln.Close()
	}()

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			select {
			case <-p.shutdownCh:
				return nil
			case <-ctx.Done():
				return nil
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		p.activeConns.Add(1)
		connID := p.connCount.Add(1)
		go p.handleConn(clientConn, connID)
	}
}

// Shutdown initiates graceful shutdown.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.once.Do(func() { close(p.shutdownCh) })

	p.listenerMu.Lock()
	ln := p.listener
	p.listenerMu.Unlock()
	if ln != nil {
		ln.Close()
	}

	done := make(chan struct{})
	go func() {
		p.activeConns.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All connections drained. Redis proxy stopped.")
	case <-ctx.Done():
	}

	if p.logger != nil {
		p.logger.Close()
	}
	return nil
}

// Addr returns the listener's address.
func (p *Proxy) Addr() string {
	p.listenerMu.Lock()
	ln := p.listener
	p.listenerMu.Unlock()
	if ln == nil {
		return ""
	}
	return ln.Addr().String()
}

// handleConn manages a single client connection.
func (p *Proxy) handleConn(clientConn net.Conn, connID int64) {
	defer p.activeConns.Done()
	defer clientConn.Close()

	if p.verbose {
		log.Printf("[conn %d] opened from %s", connID, clientConn.RemoteAddr())
	}

	// Connect to prod Redis.
	prodConn, err := net.Dial("tcp", p.prodAddr)
	if err != nil {
		log.Printf("[conn %d] failed to connect to prod Redis: %v", connID, err)
		WriteRESPValue(clientConn, BuildErrorReply("MORI failed to connect to production Redis"))
		return
	}
	defer prodConn.Close()

	// Wrap prod connection with write guard (L2).
	safeProd := NewSafeProdConn(prodConn, connID, p.verbose, p.logger)

	// Connect to shadow Redis.
	shadowConn, err := net.Dial("tcp", p.shadowAddr)
	if err != nil {
		log.Printf("[conn %d] failed to connect to shadow Redis: %v", connID, err)
		WriteRESPValue(clientConn, BuildErrorReply("MORI failed to connect to shadow Redis"))
		return
	}
	defer shadowConn.Close()

	// Auth to prod if needed.
	prodReader := bufio.NewReader(prodConn)
	shadowReader := bufio.NewReader(shadowConn)

	if p.prodPass != "" {
		authCmd := BuildCommandArray("AUTH", p.prodPass)
		if _, err := prodConn.Write(authCmd.Bytes()); err != nil {
			log.Printf("[conn %d] failed to AUTH to prod: %v", connID, err)
			return
		}
		if _, err := ReadRESPValue(prodReader); err != nil {
			log.Printf("[conn %d] failed to read AUTH response from prod: %v", connID, err)
			return
		}
	}

	// SELECT db on prod if not default.
	if p.prodDB != 0 {
		selectCmd := BuildCommandArray("SELECT", fmt.Sprintf("%d", p.prodDB))
		if _, err := prodConn.Write(selectCmd.Bytes()); err != nil {
			log.Printf("[conn %d] failed to SELECT db on prod: %v", connID, err)
			return
		}
		if _, err := ReadRESPValue(prodReader); err != nil {
			log.Printf("[conn %d] failed to read SELECT response from prod: %v", connID, err)
			return
		}
	}

	clientReader := bufio.NewReader(clientConn)

	p.routeLoop(clientConn, clientReader, safeProd, prodConn, prodReader, shadowConn, shadowReader, connID)
}

// routeLoop is the main command routing loop for a connection.
func (p *Proxy) routeLoop(
	clientConn net.Conn,
	clientReader *bufio.Reader,
	safeProd *SafeProdConn,
	rawProdConn net.Conn,
	prodReader *bufio.Reader,
	shadowConn net.Conn,
	shadowReader *bufio.Reader,
	connID int64,
) {
	for {
		// Read command from client.
		cmdVal, err := ReadRESPValue(clientReader)
		if err != nil {
			if p.verbose {
				log.Printf("[conn %d] client read error: %v", connID, err)
			}
			return
		}

		// Convert to inline command string for classification.
		inline := CommandToInline(cmdVal)
		if inline == "" {
			continue
		}

		// Handle QUIT.
		cmd, args, _ := ParseCommand(cmdVal)
		if cmd == "QUIT" {
			WriteRESPValue(clientConn, BuildSimpleString("OK"))
			return
		}

		// Handle Pub/Sub SUBSCRIBE/PSUBSCRIBE: fan-in from both backends.
		if classify.IsPubSubSubscribe(cmd) {
			p.handlePubSubSubscribe(clientConn, clientReader, cmdVal, rawProdConn, prodReader, shadowConn, shadowReader, connID)
			return // after pub/sub mode exits, close the connection
		}

		// Handle Pub/Sub UNSUBSCRIBE: forward to both.
		if classify.IsPubSubUnsubscribe(cmd) {
			cmdBytes := cmdVal.Bytes()
			shadowConn.Write(cmdBytes)
			ReadRESPValue(shadowReader)
			rawProdConn.Write(cmdBytes)
			resp, err := ReadRESPValue(prodReader)
			if err != nil {
				log.Printf("[conn %d] prod read error on unsubscribe: %v", connID, err)
				return
			}
			clientConn.Write(resp.Bytes())
			continue
		}

		// Handle EVAL/EVALSHA: hydrate keys then execute on shadow.
		if classify.IsEvalCommand(cmd) {
			p.handleEval(clientConn, cmdVal, args, rawProdConn, prodReader, shadowConn, shadowReader, connID)
			continue
		}

		// Handle SCAN: merge results from both backends when deltas exist.
		if cmd == "SCAN" {
			resp := p.executeMergedScan(cmdVal, args, rawProdConn, prodReader, shadowConn, shadowReader, connID)
			clientConn.Write(resp)
			continue
		}

		// Classify the command.
		decision := p.classifyAndRoute(inline, cmd, connID)
		cmdBytes := cmdVal.Bytes()

		switch {
		case decision.strategy == core.StrategyMergedRead || decision.strategy == core.StrategyJoinPatch:
			// Merged read: decide per-key where to read from.
			resp := p.executeMergedRead(cmdVal, decision.classification, rawProdConn, prodReader, shadowConn, shadowReader, connID)
			clientConn.Write(resp)

		case decision.target == targetProd:
			// WRITE GUARD L3: final check before prod dispatch.
			if decision.classification != nil &&
				(decision.classification.OpType == core.OpWrite || decision.classification.OpType == core.OpDDL) {
				log.Printf("[CRITICAL] [conn %d] WRITE GUARD L3: %s/%s reached targetProd — BLOCKED",
					connID, decision.classification.OpType, decision.classification.SubType)
				clientConn.Write(buildGuardErrorResponse("write operation blocked — internal routing error detected"))
				continue
			}

			// Forward to prod (through safe wrapper for L2).
			if _, err := safeProd.Write(cmdBytes); err != nil {
				log.Printf("[conn %d] L2 blocked write to prod: %v", connID, err)
				clientConn.Write(buildGuardErrorResponse("write operation blocked by production guard"))
				continue
			}
			resp, err := ReadRESPValue(prodReader)
			if err != nil {
				log.Printf("[conn %d] prod read error: %v", connID, err)
				return
			}
			clientConn.Write(resp.Bytes())

		case decision.target == targetShadow:
			// For HydrateAndWrite, copy key from prod to shadow first.
			if decision.strategy == core.StrategyHydrateAndWrite && decision.classification != nil {
				p.hydrateKeys(decision.classification, rawProdConn, prodReader, shadowConn, shadowReader, connID)
			}

			// Forward to shadow.
			if _, err := shadowConn.Write(cmdBytes); err != nil {
				log.Printf("[conn %d] shadow write error: %v", connID, err)
				return
			}
			resp, err := ReadRESPValue(shadowReader)
			if err != nil {
				log.Printf("[conn %d] shadow read error: %v", connID, err)
				return
			}
			clientConn.Write(resp.Bytes())

			// Track write effects.
			if decision.classification != nil {
				p.trackWriteEffects(decision.classification, decision.strategy, connID)
			}

		case decision.target == targetBoth:
			// Forward to both (for transactions).
			shadowConn.Write(cmdBytes)
			ReadRESPValue(shadowReader)

			rawProdConn.Write(cmdBytes)
			resp, err := ReadRESPValue(prodReader)
			if err != nil {
				log.Printf("[conn %d] prod read error: %v", connID, err)
				return
			}
			clientConn.Write(resp.Bytes())
		}
	}
}

// classifyAndRoute determines which backend should handle a command.
func (p *Proxy) classifyAndRoute(inline, cmd string, connID int64) routeDecision {
	// Meta commands always go to prod.
	if cmd == "PING" || cmd == "ECHO" || cmd == "AUTH" || cmd == "SELECT" ||
		cmd == "INFO" || cmd == "CONFIG" || cmd == "TIME" || cmd == "COMMAND" ||
		cmd == "CLIENT" || cmd == "CLUSTER" || cmd == "HELLO" || cmd == "RESET" {
		return routeDecision{target: targetProd, strategy: core.StrategyProdDirect}
	}

	classification, err := p.classifier.Classify(inline)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] classify error, forwarding to prod: %v", connID, err)
		}
		return routeDecision{target: targetProd}
	}

	strategy := p.router.Route(classification)

	// WRITE GUARD L1: validate routing decision.
	if err := validateRouteDecision(classification, strategy, connID, p.logger); err != nil {
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       core.StrategyShadowWrite,
		}
	}

	if p.verbose {
		log.Printf("[conn %d] %s/%s tables=%v → %s | %s",
			connID, classification.OpType, classification.SubType,
			classification.Tables, strategy, truncateCmd(inline, 100))
	}

	if p.logger != nil {
		p.logger.Query(connID, inline, classification, strategy, 0)
	}

	switch strategy {
	case core.StrategyShadowWrite,
		core.StrategyHydrateAndWrite,
		core.StrategyShadowDelete:
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyShadowDDL:
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyTransaction:
		return routeDecision{target: targetBoth, classification: classification, strategy: strategy}

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		return routeDecision{
			target:         targetProd,
			classification: classification,
			strategy:       strategy,
		}

	default:
		return routeDecision{target: targetProd, classification: classification, strategy: strategy}
	}
}

// executeMergedRead handles reads on keys that may have shadow overrides.
// For Redis, merged reads are per-key: check deltaMap/tombstones to decide
// whether to read from prod or shadow.
func (p *Proxy) executeMergedRead(
	cmdVal *RESPValue,
	_ *core.Classification,
	prodConn net.Conn, prodReader *bufio.Reader,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) []byte {
	cmd, args, _ := ParseCommand(cmdVal)
	cmdBytes := cmdVal.Bytes()

	// For single-key commands (GET, HGETALL, etc.), check the key directly.
	if len(args) > 0 && !isMultiKeyRead(cmd) {
		key := args[0]
		prefix := classify.KeyPrefix(key)

		// If tombstoned, return null.
		if p.tombstones != nil && p.tombstones.IsTombstoned(prefix, key) {
			return BuildNullBulkString().Bytes()
		}

		// If in deltaMap, read from shadow.
		if p.deltaMap != nil && p.deltaMap.IsDelta(prefix, key) {
			shadowConn.Write(cmdBytes)
			resp, err := ReadRESPValue(shadowReader)
			if err != nil {
				return BuildErrorReply("ERR shadow read failed").Bytes()
			}
			return resp.Bytes()
		}

		// Otherwise read from prod.
		prodConn.Write(cmdBytes)
		resp, err := ReadRESPValue(prodReader)
		if err != nil {
			return BuildErrorReply("ERR prod read failed").Bytes()
		}
		return resp.Bytes()
	}

	// For MGET, handle each key individually.
	if cmd == "MGET" && len(args) > 0 {
		return p.executeMergedMGET(args, prodConn, prodReader, shadowConn, shadowReader, connID)
	}

	// Default: forward to prod.
	prodConn.Write(cmdBytes)
	resp, err := ReadRESPValue(prodReader)
	if err != nil {
		return BuildErrorReply("ERR prod read failed").Bytes()
	}
	return resp.Bytes()
}

// executeMergedMGET handles MGET by checking each key individually.
func (p *Proxy) executeMergedMGET(
	keys []string,
	prodConn net.Conn, prodReader *bufio.Reader,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	_ int64,
) []byte {
	results := make([]RESPValue, len(keys))

	// Separate keys by source.
	var prodKeys []int
	var shadowKeys []int

	for i, key := range keys {
		prefix := classify.KeyPrefix(key)
		if p.tombstones != nil && p.tombstones.IsTombstoned(prefix, key) {
			results[i] = *BuildNullBulkString()
		} else if p.deltaMap != nil && p.deltaMap.IsDelta(prefix, key) {
			shadowKeys = append(shadowKeys, i)
		} else {
			prodKeys = append(prodKeys, i)
		}
	}

	// Batch GET from prod.
	if len(prodKeys) > 0 {
		prodArgs := make([]string, len(prodKeys)+1)
		prodArgs[0] = "MGET"
		for j, idx := range prodKeys {
			prodArgs[j+1] = keys[idx]
		}
		cmd := BuildCommandArray(prodArgs...)
		prodConn.Write(cmd.Bytes())
		resp, err := ReadRESPValue(prodReader)
		if err == nil && resp.Type == '*' && !resp.IsNull {
			for j, idx := range prodKeys {
				if j < len(resp.Array) {
					results[idx] = resp.Array[j]
				}
			}
		}
	}

	// Batch GET from shadow.
	if len(shadowKeys) > 0 {
		shadowArgs := make([]string, len(shadowKeys)+1)
		shadowArgs[0] = "MGET"
		for j, idx := range shadowKeys {
			shadowArgs[j+1] = keys[idx]
		}
		cmd := BuildCommandArray(shadowArgs...)
		shadowConn.Write(cmd.Bytes())
		resp, err := ReadRESPValue(shadowReader)
		if err == nil && resp.Type == '*' && !resp.IsNull {
			for j, idx := range shadowKeys {
				if j < len(resp.Array) {
					results[idx] = resp.Array[j]
				}
			}
		}
	}

	return (&RESPValue{Type: '*', Array: results}).Bytes()
}

// hydrateKeys copies keys from prod to shadow before a write operation.
func (p *Proxy) hydrateKeys(
	cl *core.Classification,
	prodConn net.Conn, prodReader *bufio.Reader,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) {
	// Extract keys from the classification.
	// For Redis, we use the raw command tables as key prefixes,
	// but we need the actual key. Reconstruct from RawSQL.
	args := strings.Fields(cl.RawSQL)
	if len(args) < 2 {
		return
	}
	key := args[1]
	prefix := classify.KeyPrefix(key)

	// Already in shadow? Skip.
	if p.deltaMap != nil && p.deltaMap.IsDelta(prefix, key) {
		return
	}

	// Use DUMP/RESTORE to copy.
	dumpCmd := BuildCommandArray("DUMP", key)
	prodConn.Write(dumpCmd.Bytes())
	dumpResp, err := ReadRESPValue(prodReader)
	if err != nil || dumpResp.IsNull {
		return
	}

	// Get TTL.
	pttlCmd := BuildCommandArray("PTTL", key)
	prodConn.Write(pttlCmd.Bytes())
	pttlResp, err := ReadRESPValue(prodReader)
	if err != nil {
		return
	}
	ttl := "0"
	if pttlResp.Type == ':' && pttlResp.Int > 0 {
		ttl = fmt.Sprintf("%d", pttlResp.Int)
	}

	// RESTORE to shadow (REPLACE if exists).
	restoreCmd := BuildCommandArray("RESTORE", key, ttl, dumpResp.Str, "REPLACE")
	shadowConn.Write(restoreCmd.Bytes())
	ReadRESPValue(shadowReader) // consume response

	if p.verbose {
		log.Printf("[conn %d] hydrated key %q to shadow", connID, key)
	}
}

// trackWriteEffects updates delta/tombstone state after a write operation.
func (p *Proxy) trackWriteEffects(cl *core.Classification, strategy core.RoutingStrategy, connID int64) {
	// Extract affected keys from the command.
	args := strings.Fields(cl.RawSQL)
	keys := extractWriteKeys(args)

	switch strategy {
	case core.StrategyShadowWrite:
		for _, table := range cl.Tables {
			p.deltaMap.MarkInserted(table)
		}
		for _, key := range keys {
			prefix := classify.KeyPrefix(key)
			p.deltaMap.Add(prefix, key)
		}

	case core.StrategyHydrateAndWrite:
		for _, key := range keys {
			prefix := classify.KeyPrefix(key)
			p.deltaMap.Add(prefix, key)
		}
		if err := delta.WriteDeltaMap(p.moriDir, p.deltaMap); err != nil {
			if p.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", connID, err)
			}
		}

	case core.StrategyShadowDelete:
		for _, key := range keys {
			prefix := classify.KeyPrefix(key)
			p.tombstones.Add(prefix, key)
			p.deltaMap.Remove(prefix, key)
		}
		if err := delta.WriteTombstoneSet(p.moriDir, p.tombstones); err != nil {
			if p.verbose {
				log.Printf("[conn %d] failed to persist tombstone set: %v", connID, err)
			}
		}

	case core.StrategyShadowDDL:
		// DDL (FLUSHDB, etc.) — nothing specific to track.
	}
}

// extractWriteKeys returns the keys affected by a write command.
// For MSET/MSETNX, keys are at odd positions (cmd key val key val ...).
// For other commands, the key is the second arg.
func extractWriteKeys(args []string) []string {
	if len(args) < 2 {
		return nil
	}
	cmd := strings.ToUpper(args[0])
	if cmd == "MSET" || cmd == "MSETNX" {
		var keys []string
		for i := 1; i < len(args); i += 2 {
			keys = append(keys, args[i])
		}
		return keys
	}
	return []string{args[1]}
}

func isMultiKeyRead(cmd string) bool {
	return cmd == "MGET" || cmd == "EXISTS" || cmd == "SDIFF" || cmd == "SINTER" || cmd == "SUNION"
}

func truncateCmd(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// ---------------------------------------------------------------------------
// Pub/Sub: fan-in multiplexer
// ---------------------------------------------------------------------------

// handlePubSubSubscribe forwards SUBSCRIBE/PSUBSCRIBE to both Prod and Shadow,
// then multiplexes messages from both backends to the client. The connection
// stays in pub/sub mode until all channels are unsubscribed or the client disconnects.
func (p *Proxy) handlePubSubSubscribe(
	clientConn net.Conn,
	clientReader *bufio.Reader,
	cmdVal *RESPValue,
	prodConn net.Conn, prodReader *bufio.Reader,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) {
	cmdBytes := cmdVal.Bytes()

	// Send SUBSCRIBE to both backends.
	prodConn.Write(cmdBytes)
	shadowConn.Write(cmdBytes)

	// Read the initial subscription confirmation from prod (return to client).
	resp, err := ReadRESPValue(prodReader)
	if err != nil {
		log.Printf("[conn %d] pub/sub prod subscribe error: %v", connID, err)
		return
	}
	clientConn.Write(resp.Bytes())

	// Consume shadow's subscription confirmation (discard — client already got prod's).
	ReadRESPValue(shadowReader)

	if p.verbose {
		log.Printf("[conn %d] entered pub/sub mode (fan-in from both backends)", connID)
	}

	// Fan-in: forward messages from either backend to client.
	// Use a channel to merge messages from both sources.
	msgCh := make(chan []byte, 64)
	done := make(chan struct{})

	// Goroutine: read from prod.
	go func() {
		for {
			val, err := ReadRESPValue(prodReader)
			if err != nil {
				close(done)
				return
			}
			select {
			case msgCh <- val.Bytes():
			case <-done:
				return
			}
		}
	}()

	// Goroutine: read from shadow.
	go func() {
		for {
			val, err := ReadRESPValue(shadowReader)
			if err != nil {
				return
			}
			select {
			case msgCh <- val.Bytes():
			case <-done:
				return
			}
		}
	}()

	// Goroutine: read client commands (for UNSUBSCRIBE).
	clientCh := make(chan *RESPValue, 8)
	go func() {
		for {
			val, err := ReadRESPValue(clientReader)
			if err != nil {
				close(done)
				return
			}
			select {
			case clientCh <- val:
			case <-done:
				return
			}
		}
	}()

	// Main loop: forward messages and handle unsubscribe.
	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				return
			}
			if _, err := clientConn.Write(msg); err != nil {
				return
			}
		case val, ok := <-clientCh:
			if !ok {
				return
			}
			cmd, _, _ := ParseCommand(val)
			unsub := cmdVal.Bytes()
			if classify.IsPubSubUnsubscribe(cmd) {
				unsub = val.Bytes()
			}
			// Forward unsubscribe to both backends.
			prodConn.Write(unsub)
			shadowConn.Write(unsub)
			// If it's a full unsubscribe or QUIT, exit pub/sub mode.
			if classify.IsPubSubUnsubscribe(cmd) || cmd == "QUIT" {
				return
			}
		case <-done:
			return
		case <-p.shutdownCh:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// EVAL/EVALSHA: hydrate keys, execute on shadow, track deltas
// ---------------------------------------------------------------------------

// handleEval processes EVAL/EVALSHA commands by hydrating declared keys from
// prod to shadow before execution, then tracking all keys in the delta map.
func (p *Proxy) handleEval(
	clientConn net.Conn,
	cmdVal *RESPValue,
	args []string,
	prodConn net.Conn, prodReader *bufio.Reader,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) {
	// Extract KEYS from the command.
	evalKeys := classify.ExtractEvalKeys(args)

	// Hydrate each key from prod to shadow if not already present.
	for _, key := range evalKeys {
		prefix := classify.KeyPrefix(key)
		if p.deltaMap != nil && p.deltaMap.IsDelta(prefix, key) {
			continue
		}

		// DUMP from prod.
		dumpCmd := BuildCommandArray("DUMP", key)
		prodConn.Write(dumpCmd.Bytes())
		dumpResp, err := ReadRESPValue(prodReader)
		if err != nil || dumpResp.IsNull {
			continue
		}

		// Get TTL from prod.
		pttlCmd := BuildCommandArray("PTTL", key)
		prodConn.Write(pttlCmd.Bytes())
		pttlResp, err := ReadRESPValue(prodReader)
		if err != nil {
			continue
		}
		ttl := "0"
		if pttlResp.Type == ':' && pttlResp.Int > 0 {
			ttl = fmt.Sprintf("%d", pttlResp.Int)
		}

		// RESTORE to shadow.
		restoreCmd := BuildCommandArray("RESTORE", key, ttl, dumpResp.Str, "REPLACE")
		shadowConn.Write(restoreCmd.Bytes())
		ReadRESPValue(shadowReader)

		if p.verbose {
			log.Printf("[conn %d] EVAL hydrated key %q to shadow", connID, key)
		}
	}

	// Execute EVAL on shadow.
	cmdBytes := cmdVal.Bytes()
	if _, err := shadowConn.Write(cmdBytes); err != nil {
		log.Printf("[conn %d] shadow EVAL write error: %v", connID, err)
		clientConn.Write(BuildErrorReply("ERR shadow EVAL failed").Bytes())
		return
	}
	resp, err := ReadRESPValue(shadowReader)
	if err != nil {
		log.Printf("[conn %d] shadow EVAL read error: %v", connID, err)
		clientConn.Write(BuildErrorReply("ERR shadow EVAL failed").Bytes())
		return
	}
	clientConn.Write(resp.Bytes())

	// Track all EVAL keys in delta map.
	for _, key := range evalKeys {
		prefix := classify.KeyPrefix(key)
		if p.deltaMap != nil {
			p.deltaMap.Add(prefix, key)
		}
	}
	if len(evalKeys) > 0 && p.deltaMap != nil {
		if err := delta.WriteDeltaMap(p.moriDir, p.deltaMap); err != nil {
			if p.verbose {
				log.Printf("[conn %d] failed to persist delta map after EVAL: %v", connID, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// SCAN consistency: merge results from prod and shadow
// ---------------------------------------------------------------------------

// executeMergedScan runs SCAN on both backends when shadow has deltas,
// merges the results, deduplicates, and filters tombstoned keys.
func (p *Proxy) executeMergedScan(
	cmdVal *RESPValue,
	args []string,
	prodConn net.Conn, prodReader *bufio.Reader,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) []byte {
	// If no deltas exist, just forward to prod.
	if p.deltaMap == nil || !p.deltaMap.HasAnyDelta() {
		cmdBytes := cmdVal.Bytes()
		prodConn.Write(cmdBytes)
		resp, err := ReadRESPValue(prodReader)
		if err != nil {
			return BuildErrorReply("ERR prod SCAN failed").Bytes()
		}
		return resp.Bytes()
	}

	cmdBytes := cmdVal.Bytes()

	// Run SCAN on prod.
	prodConn.Write(cmdBytes)
	prodResp, err := ReadRESPValue(prodReader)
	if err != nil {
		return BuildErrorReply("ERR prod SCAN failed").Bytes()
	}

	// Run SCAN on shadow.
	shadowConn.Write(cmdBytes)
	shadowResp, err := ReadRESPValue(shadowReader)
	if err != nil {
		return BuildErrorReply("ERR shadow SCAN failed").Bytes()
	}

	// Parse SCAN responses: *2 [$cursor, *N [key1, key2, ...]]
	prodCursor, prodKeys := parseScanResponse(prodResp)
	shadowCursor, shadowKeys := parseScanResponse(shadowResp)

	// Merge and deduplicate keys.
	seen := make(map[string]bool)
	var merged []string
	for _, key := range prodKeys {
		prefix := classify.KeyPrefix(key)
		// Skip tombstoned keys.
		if p.tombstones != nil && p.tombstones.IsTombstoned(prefix, key) {
			continue
		}
		if !seen[key] {
			seen[key] = true
			merged = append(merged, key)
		}
	}
	for _, key := range shadowKeys {
		if !seen[key] {
			seen[key] = true
			merged = append(merged, key)
		}
	}

	// Use the cursor from prod as the return cursor. When both cursors are "0",
	// the scan is complete. Otherwise, prefer the non-zero cursor.
	cursor := prodCursor
	if cursor == "0" && shadowCursor != "0" {
		cursor = shadowCursor
	}

	// Build response: *2 [$cursor, *N [keys...]]
	keyValues := make([]RESPValue, len(merged))
	for i, k := range merged {
		keyValues[i] = RESPValue{Type: '$', Str: k}
	}

	return (&RESPValue{
		Type: '*',
		Array: []RESPValue{
			{Type: '$', Str: cursor},
			{Type: '*', Array: keyValues},
		},
	}).Bytes()
}

// parseScanResponse extracts cursor and keys from a SCAN response.
func parseScanResponse(resp *RESPValue) (string, []string) {
	if resp == nil || resp.Type != '*' || len(resp.Array) < 2 {
		return "0", nil
	}
	cursor := resp.Array[0].Str
	keysArray := resp.Array[1]
	if keysArray.Type != '*' {
		return cursor, nil
	}
	keys := make([]string, len(keysArray.Array))
	for i, v := range keysArray.Array {
		keys[i] = v.Str
	}
	return cursor, keys
}
