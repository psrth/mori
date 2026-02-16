package classify

import (
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/redis/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*RedisClassifier)(nil)

// opInfo holds the classification details for a Redis command.
type opInfo struct {
	opType  core.OpType
	subType core.SubType
	keyPos  int // 1-based position of the first key argument; 0 = no key
	multiKey bool // true if command takes multiple keys (MGET, DEL, etc.)
}

// commandMap maps uppercase Redis command names to their classification.
var commandMap = map[string]opInfo{
	// --- Reads: string ---
	"GET":       {core.OpRead, core.SubSelect, 1, false},
	"MGET":      {core.OpRead, core.SubSelect, 1, true},
	"GETRANGE":  {core.OpRead, core.SubSelect, 1, false},
	"STRLEN":    {core.OpRead, core.SubSelect, 1, false},
	"EXISTS":    {core.OpRead, core.SubSelect, 1, true},
	"TYPE":      {core.OpRead, core.SubSelect, 1, false},
	"TTL":       {core.OpRead, core.SubSelect, 1, false},
	"PTTL":      {core.OpRead, core.SubSelect, 1, false},
	"KEYS":      {core.OpRead, core.SubSelect, 0, false},
	"SCAN":      {core.OpRead, core.SubSelect, 0, false},
	"DBSIZE":    {core.OpRead, core.SubSelect, 0, false},
	"RANDOMKEY": {core.OpRead, core.SubSelect, 0, false},
	"OBJECT":    {core.OpRead, core.SubSelect, 0, false},
	"DUMP":      {core.OpRead, core.SubSelect, 1, false},
	"GETDEL":    {core.OpWrite, core.SubDelete, 1, false},

	// --- Reads: hash ---
	"HGET":      {core.OpRead, core.SubSelect, 1, false},
	"HGETALL":   {core.OpRead, core.SubSelect, 1, false},
	"HMGET":     {core.OpRead, core.SubSelect, 1, false},
	"HKEYS":     {core.OpRead, core.SubSelect, 1, false},
	"HVALS":     {core.OpRead, core.SubSelect, 1, false},
	"HLEN":      {core.OpRead, core.SubSelect, 1, false},
	"HEXISTS":   {core.OpRead, core.SubSelect, 1, false},
	"HSCAN":     {core.OpRead, core.SubSelect, 1, false},
	"HRANDFIELD": {core.OpRead, core.SubSelect, 1, false},

	// --- Reads: list ---
	"LRANGE": {core.OpRead, core.SubSelect, 1, false},
	"LINDEX": {core.OpRead, core.SubSelect, 1, false},
	"LLEN":   {core.OpRead, core.SubSelect, 1, false},
	"LPOS":   {core.OpRead, core.SubSelect, 1, false},

	// --- Reads: set ---
	"SMEMBERS":    {core.OpRead, core.SubSelect, 1, false},
	"SISMEMBER":   {core.OpRead, core.SubSelect, 1, false},
	"SMISMEMBER":  {core.OpRead, core.SubSelect, 1, false},
	"SCARD":       {core.OpRead, core.SubSelect, 1, false},
	"SRANDMEMBER": {core.OpRead, core.SubSelect, 1, false},
	"SSCAN":       {core.OpRead, core.SubSelect, 1, false},
	"SDIFF":       {core.OpRead, core.SubSelect, 1, true},
	"SINTER":      {core.OpRead, core.SubSelect, 1, true},
	"SUNION":      {core.OpRead, core.SubSelect, 1, true},

	// --- Reads: sorted set ---
	"ZRANGE":           {core.OpRead, core.SubSelect, 1, false},
	"ZRANGEBYSCORE":    {core.OpRead, core.SubSelect, 1, false},
	"ZRANGEBYLEX":      {core.OpRead, core.SubSelect, 1, false},
	"ZREVRANGE":        {core.OpRead, core.SubSelect, 1, false},
	"ZREVRANGEBYSCORE": {core.OpRead, core.SubSelect, 1, false},
	"ZREVRANGEBYLEX":   {core.OpRead, core.SubSelect, 1, false},
	"ZRANK":            {core.OpRead, core.SubSelect, 1, false},
	"ZREVRANK":         {core.OpRead, core.SubSelect, 1, false},
	"ZSCORE":           {core.OpRead, core.SubSelect, 1, false},
	"ZMSCORE":          {core.OpRead, core.SubSelect, 1, false},
	"ZCARD":            {core.OpRead, core.SubSelect, 1, false},
	"ZCOUNT":           {core.OpRead, core.SubSelect, 1, false},
	"ZLEXCOUNT":        {core.OpRead, core.SubSelect, 1, false},
	"ZSCAN":            {core.OpRead, core.SubSelect, 1, false},
	"ZRANDMEMBER":      {core.OpRead, core.SubSelect, 1, false},

	// --- Reads: stream ---
	"XRANGE":    {core.OpRead, core.SubSelect, 1, false},
	"XREVRANGE": {core.OpRead, core.SubSelect, 1, false},
	"XLEN":      {core.OpRead, core.SubSelect, 1, false},
	"XINFO":     {core.OpRead, core.SubSelect, 0, false},
	"XREAD":     {core.OpRead, core.SubSelect, 0, false},
	"XPENDING":  {core.OpRead, core.SubSelect, 1, false},

	// --- Writes: string ---
	"SET":          {core.OpWrite, core.SubInsert, 1, false},
	"SETNX":        {core.OpWrite, core.SubInsert, 1, false},
	"SETEX":        {core.OpWrite, core.SubInsert, 1, false},
	"PSETEX":       {core.OpWrite, core.SubInsert, 1, false},
	"MSET":         {core.OpWrite, core.SubInsert, 1, true},
	"MSETNX":       {core.OpWrite, core.SubInsert, 1, true},
	"APPEND":       {core.OpWrite, core.SubInsert, 1, false},
	"INCR":         {core.OpWrite, core.SubInsert, 1, false},
	"INCRBY":       {core.OpWrite, core.SubInsert, 1, false},
	"INCRBYFLOAT":  {core.OpWrite, core.SubInsert, 1, false},
	"DECR":         {core.OpWrite, core.SubInsert, 1, false},
	"DECRBY":       {core.OpWrite, core.SubInsert, 1, false},
	"GETSET":       {core.OpWrite, core.SubInsert, 1, false},
	"SETRANGE":     {core.OpWrite, core.SubInsert, 1, false},

	// --- Writes: hash ---
	"HSET":          {core.OpWrite, core.SubInsert, 1, false},
	"HSETNX":        {core.OpWrite, core.SubInsert, 1, false},
	"HMSET":         {core.OpWrite, core.SubInsert, 1, false},
	"HINCRBY":       {core.OpWrite, core.SubInsert, 1, false},
	"HINCRBYFLOAT":  {core.OpWrite, core.SubInsert, 1, false},
	"HDEL":          {core.OpWrite, core.SubInsert, 1, false},

	// --- Writes: list ---
	"LPUSH":     {core.OpWrite, core.SubInsert, 1, false},
	"RPUSH":     {core.OpWrite, core.SubInsert, 1, false},
	"LPUSHX":    {core.OpWrite, core.SubInsert, 1, false},
	"RPUSHX":    {core.OpWrite, core.SubInsert, 1, false},
	"LPOP":      {core.OpWrite, core.SubInsert, 1, false},
	"RPOP":      {core.OpWrite, core.SubInsert, 1, false},
	"LSET":      {core.OpWrite, core.SubInsert, 1, false},
	"LINSERT":   {core.OpWrite, core.SubInsert, 1, false},
	"LTRIM":     {core.OpWrite, core.SubInsert, 1, false},
	"LREM":      {core.OpWrite, core.SubInsert, 1, false},
	"RPOPLPUSH": {core.OpWrite, core.SubInsert, 1, false},
	"LMOVE":     {core.OpWrite, core.SubInsert, 1, false},
	"BLPOP":     {core.OpWrite, core.SubInsert, 1, true},
	"BRPOP":     {core.OpWrite, core.SubInsert, 1, true},
	"BLMOVE":    {core.OpWrite, core.SubInsert, 1, false},

	// --- Writes: set ---
	"SADD":         {core.OpWrite, core.SubInsert, 1, false},
	"SREM":         {core.OpWrite, core.SubInsert, 1, false},
	"SMOVE":        {core.OpWrite, core.SubInsert, 1, false},
	"SPOP":         {core.OpWrite, core.SubInsert, 1, false},
	"SDIFFSTORE":   {core.OpWrite, core.SubInsert, 1, true},
	"SINTERSTORE":  {core.OpWrite, core.SubInsert, 1, true},
	"SUNIONSTORE":  {core.OpWrite, core.SubInsert, 1, true},

	// --- Writes: sorted set ---
	"ZADD":              {core.OpWrite, core.SubInsert, 1, false},
	"ZREM":              {core.OpWrite, core.SubInsert, 1, false},
	"ZINCRBY":           {core.OpWrite, core.SubInsert, 1, false},
	"ZREMRANGEBYSCORE":  {core.OpWrite, core.SubInsert, 1, false},
	"ZREMRANGEBYRANK":   {core.OpWrite, core.SubInsert, 1, false},
	"ZREMRANGEBYLEX":    {core.OpWrite, core.SubInsert, 1, false},
	"ZUNIONSTORE":       {core.OpWrite, core.SubInsert, 1, true},
	"ZINTERSTORE":       {core.OpWrite, core.SubInsert, 1, true},
	"ZPOPMIN":           {core.OpWrite, core.SubInsert, 1, false},
	"ZPOPMAX":           {core.OpWrite, core.SubInsert, 1, false},
	"BZPOPMIN":          {core.OpWrite, core.SubInsert, 1, true},
	"BZPOPMAX":          {core.OpWrite, core.SubInsert, 1, true},

	// --- Writes: stream ---
	"XADD":    {core.OpWrite, core.SubInsert, 1, false},
	"XDEL":    {core.OpWrite, core.SubInsert, 1, false},
	"XTRIM":   {core.OpWrite, core.SubInsert, 1, false},
	"XGROUP":  {core.OpWrite, core.SubInsert, 0, false},
	"XACK":    {core.OpWrite, core.SubInsert, 1, false},
	"XCLAIM":  {core.OpWrite, core.SubInsert, 1, false},

	// --- Writes: key management ---
	"DEL":       {core.OpWrite, core.SubDelete, 1, true},
	"UNLINK":    {core.OpWrite, core.SubDelete, 1, true},
	"RENAME":    {core.OpWrite, core.SubInsert, 1, false},
	"RENAMENX":  {core.OpWrite, core.SubInsert, 1, false},
	"COPY":      {core.OpWrite, core.SubInsert, 1, false},
	"MOVE":      {core.OpWrite, core.SubInsert, 1, false},
	"PERSIST":   {core.OpWrite, core.SubInsert, 1, false},
	"EXPIRE":    {core.OpWrite, core.SubInsert, 1, false},
	"EXPIREAT":  {core.OpWrite, core.SubInsert, 1, false},
	"PEXPIRE":   {core.OpWrite, core.SubInsert, 1, false},
	"PEXPIREAT": {core.OpWrite, core.SubInsert, 1, false},
	"RESTORE":   {core.OpWrite, core.SubInsert, 1, false},
	"SORT":      {core.OpRead, core.SubSelect, 1, false}, // read by default; write if STORE present

	// --- DDL: destructive server-level ---
	"FLUSHDB":  {core.OpDDL, core.SubOther, 0, false},
	"FLUSHALL": {core.OpDDL, core.SubOther, 0, false},
	"SWAPDB":   {core.OpDDL, core.SubOther, 0, false},

	// --- Transactions ---
	"MULTI":   {core.OpTransaction, core.SubBegin, 0, false},
	"EXEC":    {core.OpTransaction, core.SubCommit, 0, false},
	"DISCARD": {core.OpTransaction, core.SubRollback, 0, false},
	"WATCH":   {core.OpOther, core.SubOther, 1, true},
	"UNWATCH": {core.OpOther, core.SubOther, 0, false},

	// --- Meta / passthrough ---
	"PING":      {core.OpOther, core.SubOther, 0, false},
	"ECHO":      {core.OpOther, core.SubOther, 0, false},
	"QUIT":      {core.OpOther, core.SubOther, 0, false},
	"AUTH":      {core.OpOther, core.SubOther, 0, false},
	"SELECT":    {core.OpOther, core.SubOther, 0, false},
	"INFO":      {core.OpOther, core.SubOther, 0, false},
	"CONFIG":    {core.OpOther, core.SubOther, 0, false}, // handled specially below
	"TIME":      {core.OpOther, core.SubOther, 0, false},
	"COMMAND":   {core.OpOther, core.SubOther, 0, false},
	"CLIENT":    {core.OpOther, core.SubOther, 0, false},
	"CLUSTER":   {core.OpOther, core.SubOther, 0, false},
	"DEBUG":     {core.OpOther, core.SubOther, 0, false},
	"WAIT":      {core.OpOther, core.SubOther, 0, false},
	"SUBSCRIBE":    {core.OpOther, core.SubOther, 0, true},  // routed to both prod+shadow
	"PSUBSCRIBE":   {core.OpOther, core.SubOther, 0, true},  // routed to both prod+shadow
	"UNSUBSCRIBE":  {core.OpOther, core.SubOther, 0, false},
	"PUNSUBSCRIBE": {core.OpOther, core.SubOther, 0, false},
	"PUBLISH":      {core.OpWrite, core.SubInsert, 0, false}, // shadow-only (write guard)
	"PUBSUB":       {core.OpOther, core.SubOther, 0, false},

	// --- Lua scripting ---
	"EVAL":      {core.OpWrite, core.SubInsert, 0, true},
	"EVALSHA":   {core.OpWrite, core.SubInsert, 0, true},
	"EVALRO":    {core.OpRead, core.SubSelect, 0, true},
	"EVALSHA_RO": {core.OpRead, core.SubSelect, 0, true},
	"SCRIPT":    {core.OpOther, core.SubOther, 0, false},
	"SLOWLOG":   {core.OpOther, core.SubOther, 0, false},
	"MEMORY":    {core.OpOther, core.SubOther, 0, false},
	"LATENCY":   {core.OpOther, core.SubOther, 0, false},
	"MODULE":    {core.OpOther, core.SubOther, 0, false},
	"RESET":     {core.OpOther, core.SubOther, 0, false},
	"HELLO":     {core.OpOther, core.SubOther, 0, false},
}

// RedisClassifier implements core.Classifier for Redis commands.
type RedisClassifier struct {
	tables map[string]schema.KeyMeta
}

// New creates a RedisClassifier.
func New(tables map[string]schema.KeyMeta) *RedisClassifier {
	if tables == nil {
		tables = make(map[string]schema.KeyMeta)
	}
	return &RedisClassifier{tables: tables}
}

// Classify parses a Redis command string and returns its classification.
// The command string is in inline format: "SET user:1 value".
func (c *RedisClassifier) Classify(query string) (*core.Classification, error) {
	cl := &core.Classification{RawSQL: query}
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
		return cl, nil
	}

	args := splitCommand(trimmed)
	if len(args) == 0 {
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
		return cl, nil
	}

	cmd := strings.ToUpper(args[0])

	// Special case: CONFIG SET is a DDL, CONFIG GET is a read.
	if cmd == "CONFIG" && len(args) > 1 {
		sub := strings.ToUpper(args[1])
		if sub == "SET" || sub == "RESETSTAT" || sub == "REWRITE" {
			cl.OpType = core.OpDDL
			cl.SubType = core.SubOther
			return cl, nil
		}
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
		return cl, nil
	}

	// Special case: SORT with STORE is a write.
	if cmd == "SORT" {
		for _, a := range args[1:] {
			if strings.ToUpper(a) == "STORE" {
				cl.OpType = core.OpWrite
				cl.SubType = core.SubInsert
				cl.Tables = c.extractKeyPrefixes(args[1:2])
				return cl, nil
			}
		}
	}

	info, ok := commandMap[cmd]
	if !ok {
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
		return cl, nil
	}

	cl.OpType = info.opType
	cl.SubType = info.subType

	// Extract key prefixes as "tables".
	if info.keyPos > 0 && len(args) > info.keyPos {
		if info.multiKey {
			cl.Tables = c.extractKeyPrefixes(args[info.keyPos:])
		} else {
			cl.Tables = c.extractKeyPrefixes(args[info.keyPos : info.keyPos+1])
		}
	}

	// For MSET/MSETNX, keys are at odd positions (key val key val ...).
	if cmd == "MSET" || cmd == "MSETNX" {
		var keys []string
		for i := 1; i < len(args); i += 2 {
			keys = append(keys, args[i])
		}
		cl.Tables = c.extractKeyPrefixes(keys)
	}

	// For EVAL/EVALSHA, extract KEYS from numkeys argument.
	if cmd == "EVAL" || cmd == "EVALSHA" {
		evalKeys := ExtractEvalKeys(args[1:])
		if len(evalKeys) > 0 {
			cl.Tables = c.extractKeyPrefixes(evalKeys)
		}
	}

	return cl, nil
}

// ClassifyWithParams delegates to Classify (Redis has no parameterized queries).
func (c *RedisClassifier) ClassifyWithParams(query string, params []interface{}) (*core.Classification, error) {
	return c.Classify(query)
}

// extractKeyPrefixes extracts unique key prefixes from a list of Redis keys.
// E.g., "user:123" -> "user", "session:abc" -> "session".
func (c *RedisClassifier) extractKeyPrefixes(keys []string) []string {
	seen := make(map[string]bool)
	var prefixes []string
	for _, key := range keys {
		prefix := KeyPrefix(key)
		if prefix != "" && !seen[prefix] {
			seen[prefix] = true
			prefixes = append(prefixes, prefix)
		}
	}
	return prefixes
}

// KeyPrefix returns the prefix of a Redis key (segment before first ':').
// If the key has no ':', the entire key is returned.
func KeyPrefix(key string) string {
	if idx := strings.Index(key, ":"); idx >= 0 {
		return key[:idx]
	}
	return key
}

// splitCommand splits a Redis inline command string into tokens.
// Handles double-quoted strings.
func splitCommand(s string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote {
			if ch == '"' {
				inQuote = false
			} else {
				current.WriteByte(ch)
			}
			continue
		}
		if ch == '"' {
			inQuote = true
			continue
		}
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteByte(ch)
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// IsWriteCommand returns true if the given Redis command name is a write operation.
func IsWriteCommand(cmd string) bool {
	info, ok := commandMap[strings.ToUpper(cmd)]
	if !ok {
		return false
	}
	return info.opType == core.OpWrite || info.opType == core.OpDDL
}

// IsPubSubSubscribe returns true if the command is SUBSCRIBE or PSUBSCRIBE.
func IsPubSubSubscribe(cmd string) bool {
	u := strings.ToUpper(cmd)
	return u == "SUBSCRIBE" || u == "PSUBSCRIBE"
}

// IsPubSubUnsubscribe returns true if the command is UNSUBSCRIBE or PUNSUBSCRIBE.
func IsPubSubUnsubscribe(cmd string) bool {
	u := strings.ToUpper(cmd)
	return u == "UNSUBSCRIBE" || u == "PUNSUBSCRIBE"
}

// IsEvalCommand returns true if the command is EVAL or EVALSHA.
func IsEvalCommand(cmd string) bool {
	u := strings.ToUpper(cmd)
	return u == "EVAL" || u == "EVALSHA"
}

// ExtractEvalKeys extracts the KEYS arguments from an EVAL/EVALSHA command.
// EVAL script numkeys key [key ...] arg [arg ...]
// Returns the key names based on numkeys.
func ExtractEvalKeys(args []string) []string {
	// args[0] = script/sha, args[1] = numkeys, args[2..] = keys then argv
	if len(args) < 2 {
		return nil
	}
	numkeys := 0
	for _, ch := range args[1] {
		if ch < '0' || ch > '9' {
			return nil
		}
		numkeys = numkeys*10 + int(ch-'0')
	}
	if numkeys <= 0 || len(args) < 2+numkeys {
		return nil
	}
	return args[2 : 2+numkeys]
}
