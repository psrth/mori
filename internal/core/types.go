package core

import "fmt"

// OpType classifies the broad category of a SQL operation.
type OpType int

const (
	OpRead        OpType = iota // SELECT queries
	OpWrite                     // INSERT, UPDATE, DELETE
	OpDDL                       // ALTER, CREATE, DROP
	OpTransaction               // BEGIN, COMMIT, ROLLBACK
	OpOther                     // SET, SHOW, EXPLAIN, etc.
)

func (o OpType) String() string {
	switch o {
	case OpRead:
		return "READ"
	case OpWrite:
		return "WRITE"
	case OpDDL:
		return "DDL"
	case OpTransaction:
		return "TRANSACTION"
	case OpOther:
		return "OTHER"
	default:
		return fmt.Sprintf("OpType(%d)", int(o))
	}
}

// SubType is the specific operation within an OpType.
type SubType int

const (
	SubSelect   SubType = iota // SELECT
	SubInsert                   // INSERT
	SubUpdate                   // UPDATE
	SubDelete                   // DELETE
	SubAlter                    // ALTER TABLE
	SubCreate                   // CREATE TABLE / CREATE INDEX
	SubDrop                     // DROP TABLE
	SubBegin                    // BEGIN
	SubCommit                   // COMMIT
	SubRollback                 // ROLLBACK
	SubOther                    // Anything else
)

func (s SubType) String() string {
	switch s {
	case SubSelect:
		return "SELECT"
	case SubInsert:
		return "INSERT"
	case SubUpdate:
		return "UPDATE"
	case SubDelete:
		return "DELETE"
	case SubAlter:
		return "ALTER"
	case SubCreate:
		return "CREATE"
	case SubDrop:
		return "DROP"
	case SubBegin:
		return "BEGIN"
	case SubCommit:
		return "COMMIT"
	case SubRollback:
		return "ROLLBACK"
	case SubOther:
		return "OTHER"
	default:
		return fmt.Sprintf("SubType(%d)", int(s))
	}
}

// RoutingStrategy determines where a query is executed.
type RoutingStrategy int

const (
	StrategyProdDirect      RoutingStrategy = iota // Forward to Prod, return result
	StrategyMergedRead                              // Query both, filter, merge
	StrategyJoinPatch                               // Execute on Prod, patch from Shadow
	StrategyShadowWrite                             // Execute on Shadow only
	StrategyHydrateAndWrite                         // Hydrate from Prod, then write to Shadow
	StrategyShadowDelete                            // Delete from Shadow, add tombstone
	StrategyShadowDDL                               // Execute DDL on Shadow, update schema registry
	StrategyTransaction                             // Transaction control (BEGIN/COMMIT/ROLLBACK)
)

func (r RoutingStrategy) String() string {
	switch r {
	case StrategyProdDirect:
		return "PROD_DIRECT"
	case StrategyMergedRead:
		return "MERGED_READ"
	case StrategyJoinPatch:
		return "JOIN_PATCH"
	case StrategyShadowWrite:
		return "SHADOW_WRITE"
	case StrategyHydrateAndWrite:
		return "HYDRATE_AND_WRITE"
	case StrategyShadowDelete:
		return "SHADOW_DELETE"
	case StrategyShadowDDL:
		return "SHADOW_DDL"
	case StrategyTransaction:
		return "TRANSACTION"
	default:
		return fmt.Sprintf("RoutingStrategy(%d)", int(r))
	}
}

// Classification is the result of parsing a query.
type Classification struct {
	OpType       OpType   // READ, WRITE, DDL, TRANSACTION, OTHER
	SubType      SubType  // SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, DROP, BEGIN, COMMIT, ROLLBACK
	Tables       []string // All table names referenced
	PKs          []TablePK // Extractable (table, pk) pairs from WHERE clauses
	IsJoin       bool     // Whether the query involves multiple tables
	HasLimit     bool     // Whether the query has a LIMIT clause
	Limit        int      // The LIMIT value, if present
	OrderBy      string   // Raw ORDER BY clause, for re-application after merge
	HasAggregate  bool     // Whether the query uses aggregate functions (COUNT, SUM, etc.) or GROUP BY
	HasSetOp      bool     // Whether the query uses UNION/INTERSECT/EXCEPT
	IsComplexRead bool     // Whether the query is too complex for merged read (derived tables, complex CTEs)
	RawSQL        string   // Original SQL text
}

// TablePK identifies a row in a specific table.
type TablePK struct {
	Table string
	PK    string // Serialized PK value (scalar or JSON-encoded composite)
}

// Row represents a single database row as a map of column names to values.
type Row map[string]interface{}

// ResultSet is a collection of rows with column metadata.
type ResultSet struct {
	Columns []string
	Rows    []Row
}
