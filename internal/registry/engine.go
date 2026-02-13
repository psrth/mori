package registry

// EngineID uniquely identifies a database engine.
type EngineID string

const (
	Postgres      EngineID = "postgres"
	CockroachDB   EngineID = "cockroachdb"
	MySQL         EngineID = "mysql"
	MariaDB       EngineID = "mariadb"
	MSSQL         EngineID = "mssql"
	Oracle        EngineID = "oracle"
	SQLite        EngineID = "sqlite"
	Redis         EngineID = "redis"
	MongoDB       EngineID = "mongodb"
	Elasticsearch EngineID = "elasticsearch"
	DynamoDB      EngineID = "dynamodb"
	Firestore     EngineID = "firestore"
	Cassandra     EngineID = "cassandra"
	ClickHouse    EngineID = "clickhouse"
	Neo4j         EngineID = "neo4j"
)

// EngineTier groups engines by protocol maturity and priority.
type EngineTier int

const (
	EngineTierT1 EngineTier = 1 // SQL / pgwire-compatible
	EngineTierT2 EngineTier = 2 // SQL / new wire protocols
	EngineTierT3 EngineTier = 3 // NoSQL
	EngineTierT4 EngineTier = 4 // Specialized
)

// Engine describes a database engine and its basic properties.
type Engine struct {
	ID          EngineID
	DisplayName string
	Tier        EngineTier
	Category    string // e.g. "SQL/PGWIRE", "NOSQL"
	DefaultPort int    // 0 means not applicable (e.g. SQLite, DynamoDB)
	Supported   bool   // true if mori can actually proxy this engine today
}

var engines = []Engine{
	// T1 — SQL / pgwire-compatible
	{Postgres, "PostgreSQL", EngineTierT1, "SQL/PGWIRE", 5432, true},
	{CockroachDB, "CockroachDB", EngineTierT1, "SQL/PGWIRE", 26257, true},
	{MySQL, "MySQL", EngineTierT1, "SQL/MYSQL", 3306, true},
	{MariaDB, "MariaDB", EngineTierT1, "SQL/MYSQL", 3306, true},

	// T2 — SQL / new wire protocols
	{MSSQL, "MS SQL Server", EngineTierT2, "SQL/TDS", 1433, true},
	{Oracle, "Oracle", EngineTierT2, "SQL/NET8", 1521, true},
	{SQLite, "SQLite", EngineTierT2, "SQL/EMBEDDED", 0, true},

	// T3 — NoSQL
	{Redis, "Redis", EngineTierT3, "NOSQL", 6379, false},
	{MongoDB, "MongoDB", EngineTierT3, "NOSQL", 27017, false},
	{Elasticsearch, "Elasticsearch", EngineTierT3, "NOSQL", 9200, false},
	{DynamoDB, "DynamoDB", EngineTierT3, "NOSQL", 0, false},
	{Firestore, "Firestore", EngineTierT3, "NOSQL", 0, false},

	// T4 — Specialized
	{Cassandra, "Cassandra / ScyllaDB", EngineTierT4, "SPECIALIZED", 9042, false},
	{ClickHouse, "ClickHouse", EngineTierT4, "SPECIALIZED", 9000, false},
	{Neo4j, "Neo4j", EngineTierT4, "SPECIALIZED", 7687, false},
}

// AllEngines returns every registered engine in tier order.
func AllEngines() []Engine {
	out := make([]Engine, len(engines))
	copy(out, engines)
	return out
}

// EngineByID looks up an engine by its ID. Returns false if not found.
func EngineByID(id EngineID) (Engine, bool) {
	for _, e := range engines {
		if e.ID == id {
			return e, true
		}
	}
	return Engine{}, false
}

// SupportedEngines returns only engines where Supported is true.
func SupportedEngines() []Engine {
	var out []Engine
	for _, e := range engines {
		if e.Supported {
			out = append(out, e)
		}
	}
	return out
}

// EngineTierLabel returns a human-readable label for a tier.
func EngineTierLabel(t EngineTier) string {
	switch t {
	case EngineTierT1:
		return "SQL (pgwire & mysql)"
	case EngineTierT2:
		return "SQL (other)"
	case EngineTierT3:
		return "NoSQL"
	case EngineTierT4:
		return "Specialized"
	default:
		return "Unknown"
	}
}
