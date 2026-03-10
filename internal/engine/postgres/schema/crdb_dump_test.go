package schema

import (
	"strings"
	"testing"
)

func TestCleanCRDBSchema_StripsFamilyClauses(t *testing.T) {
	input := `CREATE TABLE users (
	id BIGINT NOT NULL,
	name TEXT,
	FAMILY "primary" (id, name)
);`
	got := CleanCRDBSchema(input)
	if strings.Contains(got, "FAMILY") {
		t.Errorf("expected FAMILY clause to be stripped, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_StripsUniqueRowID(t *testing.T) {
	input := `CREATE TABLE users (
	id BIGINT NOT NULL DEFAULT unique_rowid(),
	name TEXT
);`
	got := CleanCRDBSchema(input)
	if strings.Contains(strings.ToLower(got), "unique_rowid") {
		t.Errorf("expected unique_rowid() to be stripped, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_StripsInterleave(t *testing.T) {
	input := `CREATE TABLE orders (
	id BIGINT NOT NULL,
	user_id BIGINT
) INTERLEAVE IN PARENT users (user_id);`
	got := CleanCRDBSchema(input)
	if strings.Contains(strings.ToUpper(got), "INTERLEAVE") {
		t.Errorf("expected INTERLEAVE to be stripped, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_StripsConfigureZone(t *testing.T) {
	input := `CREATE TABLE users (id BIGINT NOT NULL);
ALTER TABLE users CONFIGURE ZONE USING num_replicas = 3;
`
	got := CleanCRDBSchema(input)
	if strings.Contains(strings.ToUpper(got), "CONFIGURE ZONE") {
		t.Errorf("expected CONFIGURE ZONE to be stripped, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_StripsHashSharded(t *testing.T) {
	input := `CREATE INDEX idx ON users (name) USING HASH WITH BUCKET_COUNT = 8;`
	got := CleanCRDBSchema(input)
	if strings.Contains(strings.ToUpper(got), "BUCKET_COUNT") {
		t.Errorf("expected hash shard syntax to be stripped, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_StripsForeignKeys(t *testing.T) {
	input := `ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id);
`
	got := CleanCRDBSchema(input)
	if strings.Contains(strings.ToUpper(got), "FOREIGN KEY") {
		t.Errorf("expected FK constraint to be stripped, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_StripsValidateConstraint(t *testing.T) {
	input := `ALTER TABLE orders VALIDATE CONSTRAINT fk_user;
`
	got := CleanCRDBSchema(input)
	if strings.Contains(strings.ToUpper(got), "VALIDATE CONSTRAINT") {
		t.Errorf("expected VALIDATE CONSTRAINT to be stripped, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_NormalizesTypes(t *testing.T) {
	input := `CREATE TABLE t (a INT8, b INT4, c INT2, d FLOAT4, e FLOAT8, f STRING);`
	got := CleanCRDBSchema(input)
	if strings.Contains(got, "INT8") {
		t.Errorf("expected INT8 to be replaced with BIGINT, got:\n%s", got)
	}
	if strings.Contains(got, "INT4") {
		t.Errorf("expected INT4 to be replaced with INTEGER, got:\n%s", got)
	}
	if strings.Contains(got, "INT2") {
		t.Errorf("expected INT2 to be replaced with SMALLINT, got:\n%s", got)
	}
	if strings.Contains(got, "FLOAT4") {
		t.Errorf("expected FLOAT4 to be replaced with REAL, got:\n%s", got)
	}
	if strings.Contains(got, "FLOAT8") {
		t.Errorf("expected FLOAT8 to be replaced with DOUBLE PRECISION, got:\n%s", got)
	}
	if strings.Contains(got, "STRING") {
		t.Errorf("expected STRING to be replaced with TEXT, got:\n%s", got)
	}
	if !strings.Contains(got, "BIGINT") {
		t.Errorf("expected BIGINT in output, got:\n%s", got)
	}
	if !strings.Contains(got, "INTEGER") {
		t.Errorf("expected INTEGER in output, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_CleansTrailingCommas(t *testing.T) {
	input := `CREATE TABLE t (
	id BIGINT NOT NULL,
	FAMILY "primary" (id)
);`
	got := CleanCRDBSchema(input)
	if strings.Contains(got, ",\n)") || strings.Contains(got, ", )") {
		t.Errorf("expected trailing comma to be cleaned, got:\n%s", got)
	}
}

func TestCRDBClassifyPKType_UniqueRowID(t *testing.T) {
	tests := []struct {
		name     string
		types    []string
		defaults []string
		want     string
	}{
		{"bigint unique_rowid", []string{"bigint"}, []string{"unique_rowid()"}, "bigserial"},
		{"int8 unique_rowid", []string{"int8"}, []string{"unique_rowid()"}, "bigserial"},
		{"integer unique_rowid", []string{"integer"}, []string{"unique_rowid()"}, "serial"},
		{"smallint unique_rowid", []string{"smallint"}, []string{"unique_rowid()"}, "smallserial"},
		{"int2 unique_rowid", []string{"int2"}, []string{"unique_rowid()"}, "smallserial"},
		{"uuid", []string{"uuid"}, []string{"gen_random_uuid()"}, "uuid"},
		{"composite", []string{"bigint", "bigint"}, []string{"", ""}, "composite"},
		{"nextval fallback", []string{"bigint"}, []string{"nextval('seq')"}, "bigserial"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CRDBClassifyPKType(tt.types, tt.defaults)
			if got != tt.want {
				t.Errorf("CRDBClassifyPKType(%v, %v) = %q, want %q", tt.types, tt.defaults, got, tt.want)
			}
		})
	}
}

func TestCleanCRDBSchema_StripsSplitAt(t *testing.T) {
	input := `ALTER TABLE users SPLIT AT VALUES (100), (200);
`
	got := CleanCRDBSchema(input)
	if strings.Contains(strings.ToUpper(got), "SPLIT AT") {
		t.Errorf("expected SPLIT AT to be stripped, got:\n%s", got)
	}
}

func TestCleanCRDBSchema_ComplexTable(t *testing.T) {
	// Realistic CockroachDB output with multiple features to strip.
	input := `CREATE TABLE users (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	name STRING NOT NULL,
	email STRING,
	CONSTRAINT users_pkey PRIMARY KEY (id ASC),
	FAMILY "primary" (id, name, email)
);
CREATE TABLE orders (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	user_id INT8 NOT NULL,
	total INT8,
	CONSTRAINT orders_pkey PRIMARY KEY (id ASC),
	CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id),
	FAMILY "primary" (id, user_id, total)
);
ALTER TABLE orders VALIDATE CONSTRAINT fk_user;
`
	got := CleanCRDBSchema(input)

	// Should not contain CRDB-specific syntax
	for _, bad := range []string{"FAMILY", "unique_rowid", "FOREIGN KEY", "VALIDATE CONSTRAINT", "INT8", "STRING"} {
		if strings.Contains(got, bad) {
			t.Errorf("expected %q to be stripped from output, got:\n%s", bad, got)
		}
	}

	// Should contain PG-compatible replacements
	for _, good := range []string{"BIGINT", "TEXT", "CREATE TABLE users", "CREATE TABLE orders"} {
		if !strings.Contains(got, good) {
			t.Errorf("expected %q in output, got:\n%s", good, got)
		}
	}
}
