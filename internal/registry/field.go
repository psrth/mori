package registry

import (
	"fmt"
	"strconv"
)

// ConnectionField describes a single parameter needed to connect to an engine.
type ConnectionField struct {
	Key         string                // yaml/config key, e.g. "host"
	Label       string                // display label, e.g. "Hostname"
	Required    bool                  // must be provided
	Default     string                // default value (empty = none)
	Sensitive   bool                  // mask input (passwords, keys)
	Placeholder string                // hint shown in prompt
	Validate    func(string) error    // optional validation (nil = accept anything)
}

// validateNonEmpty returns an error if the value is empty.
func validateNonEmpty(label string) func(string) error {
	return func(v string) error {
		if v == "" {
			return fmt.Errorf("%s is required", label)
		}
		return nil
	}
}

// validatePort returns an error if the value is not a valid port number.
func validatePort(v string) error {
	if v == "" {
		return nil // optional or has default
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port must be a number between 1 and 65535")
	}
	return nil
}

// Common field templates reused across engines.
var (
	fieldHost = ConnectionField{
		Key: "host", Label: "Host", Required: true,
		Placeholder: "db.example.com",
		Validate:    validateNonEmpty("host"),
	}
	fieldPort = ConnectionField{
		Key: "port", Label: "Port", Required: false,
		Placeholder: "5432",
		Validate:    validatePort,
	}
	fieldUser = ConnectionField{
		Key: "user", Label: "User", Required: true,
		Default:     "postgres",
		Placeholder: "postgres",
		Validate:    validateNonEmpty("user"),
	}
	fieldPassword = ConnectionField{
		Key: "password", Label: "Password", Required: false,
		Sensitive:   true,
		Placeholder: "enter password",
	}
	fieldDatabase = ConnectionField{
		Key: "database", Label: "Database", Required: true,
		Placeholder: "mydb",
		Validate:    validateNonEmpty("database"),
	}
	fieldSSLMode = ConnectionField{
		Key: "ssl_mode", Label: "SSL Mode", Required: false,
		Default:     "disable",
		Placeholder: "disable | require | verify-full",
	}
	fieldSSL = ConnectionField{
		Key: "ssl", Label: "SSL", Required: false,
		Default:     "false",
		Placeholder: "true | false",
	}
)

// engineFields maps each engine to its ordered list of connection fields.
var engineFields = map[EngineID][]ConnectionField{
	Postgres: {
		fieldHost,
		withDefault(fieldPort, "5432"),
		fieldUser,
		fieldPassword,
		fieldDatabase,
		fieldSSLMode,
	},
	CockroachDB: {
		fieldHost,
		withDefault(fieldPort, "26257"),
		fieldUser,
		fieldPassword,
		fieldDatabase,
		fieldSSLMode,
	},
	MySQL: {
		fieldHost,
		withDefault(fieldPort, "3306"),
		withDefault(fieldUser, "root"),
		fieldPassword,
		fieldDatabase,
		fieldSSLMode,
	},
	MariaDB: {
		fieldHost,
		withDefault(fieldPort, "3306"),
		withDefault(fieldUser, "root"),
		fieldPassword,
		fieldDatabase,
		fieldSSLMode,
	},
	MSSQL: {
		fieldHost,
		withDefault(fieldPort, "1433"),
		withDefault(fieldUser, "sa"),
		fieldPassword,
		fieldDatabase,
		{Key: "encrypt", Label: "Encrypt", Default: "true", Placeholder: "true | false"},
		{Key: "trust_server_cert", Label: "Trust Server Certificate", Default: "false", Placeholder: "true | false"},
	},
	SQLite: {
		{Key: "file_path", Label: "File Path", Required: true, Placeholder: "./data.db", Validate: validateNonEmpty("file path")},
	},
	Redis: {
		fieldHost,
		withDefault(fieldPort, "6379"),
		fieldPassword,
		{Key: "db_number", Label: "DB Number", Default: "0", Placeholder: "0-15"},
		fieldSSL,
	},
	MongoDB: {
		fieldHost,
		withDefault(fieldPort, "27017"),
		fieldUser,
		fieldPassword,
		fieldDatabase,
		{Key: "auth_source", Label: "Auth Source", Default: "admin", Placeholder: "admin"},
		fieldSSL,
	},
	Elasticsearch: {
		fieldHost,
		withDefault(fieldPort, "9200"),
		fieldUser,
		fieldPassword,
		{Key: "index", Label: "Index", Required: false, Placeholder: "my-index"},
		fieldSSL,
	},
	DynamoDB: {
		{Key: "region", Label: "AWS Region", Required: true, Placeholder: "us-east-1", Validate: validateNonEmpty("region")},
		{Key: "access_key_id", Label: "Access Key ID", Required: true, Sensitive: true, Validate: validateNonEmpty("access key")},
		{Key: "secret_access_key", Label: "Secret Access Key", Required: true, Sensitive: true, Validate: validateNonEmpty("secret key")},
		{Key: "endpoint", Label: "Endpoint (local dev)", Placeholder: "http://localhost:8000"},
	},
	Firestore: {
		{Key: "project_id", Label: "GCP Project ID", Required: true, Placeholder: "my-project", Validate: validateNonEmpty("project ID")},
		{Key: "credentials_file", Label: "Credentials JSON Path", Required: false, Placeholder: "./service-account.json"},
	},
	Cassandra: {
		{Key: "hosts", Label: "Contact Points (comma-separated)", Required: true, Placeholder: "node1,node2,node3", Validate: validateNonEmpty("hosts")},
		withDefault(fieldPort, "9042"),
		fieldUser,
		fieldPassword,
		{Key: "keyspace", Label: "Keyspace", Required: true, Placeholder: "my_keyspace", Validate: validateNonEmpty("keyspace")},
		{Key: "datacenter", Label: "Datacenter", Placeholder: "dc1"},
	},
	ClickHouse: {
		fieldHost,
		withDefault(fieldPort, "9000"),
		withDefault(fieldUser, "default"),
		fieldPassword,
		fieldDatabase,
	},
	Neo4j: {
		fieldHost,
		withDefault(fieldPort, "7687"),
		withDefault(fieldUser, "neo4j"),
		fieldPassword,
		{Key: "database", Label: "Database", Default: "neo4j", Placeholder: "neo4j"},
	},
}

// FieldsForEngine returns the connection fields required for the given engine.
// Returns nil if the engine is unknown.
func FieldsForEngine(id EngineID) []ConnectionField {
	fields, ok := engineFields[id]
	if !ok {
		return nil
	}
	out := make([]ConnectionField, len(fields))
	copy(out, fields)
	return out
}

// withDefault returns a copy of the field with a different default value.
func withDefault(f ConnectionField, def string) ConnectionField {
	f.Default = def
	if f.Placeholder == "" {
		f.Placeholder = def
	}
	return f
}
