package schema

import (
	"testing"
)

func TestStripForeignKeys(t *testing.T) {
	input := `CREATE TABLE users (
    id serial PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE orders (
    id serial PRIMARY KEY,
    user_id integer NOT NULL,
    total numeric
);

ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);

ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id);
`

	result := StripForeignKeys(input)

	if containsFK(result) {
		t.Errorf("FK constraint was not removed:\n%s", result)
	}
	if !contains(result, "orders_pkey PRIMARY KEY") {
		t.Error("PRIMARY KEY constraint was incorrectly removed")
	}
	if !contains(result, "CREATE TABLE users") {
		t.Error("users CREATE TABLE was removed")
	}
	if !contains(result, "CREATE TABLE orders") {
		t.Error("orders CREATE TABLE was removed")
	}
}

func TestStripForeignKeysMultipleFK(t *testing.T) {
	input := `ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_product_fk FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE CASCADE;
ALTER TABLE ONLY public.reviews ADD CONSTRAINT reviews_order_fk FOREIGN KEY (order_id) REFERENCES public.orders(id);
`

	result := StripForeignKeys(input)
	if containsFK(result) {
		t.Errorf("FK constraints were not fully removed:\n%s", result)
	}
}

func TestStripForeignKeysPreservesOtherConstraints(t *testing.T) {
	input := `ALTER TABLE ONLY public.users ADD CONSTRAINT users_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.users ADD CONSTRAINT users_email_key UNIQUE (email);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_total_check CHECK (total >= 0);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id);
`

	result := StripForeignKeys(input)
	if !contains(result, "users_pkey PRIMARY KEY") {
		t.Error("PRIMARY KEY was removed")
	}
	if !contains(result, "users_email_key UNIQUE") {
		t.Error("UNIQUE was removed")
	}
	if !contains(result, "orders_total_check CHECK") {
		t.Error("CHECK was removed")
	}
	if containsFK(result) {
		t.Error("FK was not removed")
	}
}

func TestStripForeignKeysNoFK(t *testing.T) {
	input := `CREATE TABLE users (id serial PRIMARY KEY, name text);`
	result := StripForeignKeys(input)
	if result != input {
		t.Errorf("input was modified when no FKs present:\n%s", result)
	}
}

func TestComputeOffset(t *testing.T) {
	tests := []struct {
		prodMax int64
		want    int64
	}{
		{0, 10_000_000},
		{100, 10_000_100},
		{834_219, 10_834_219},
		{5_000_000, 50_000_000},
		{100_000_000, 1_000_000_000},
		{1, 10_000_001},
	}

	for _, tt := range tests {
		got := computeOffset(tt.prodMax)
		if got != tt.want {
			t.Errorf("computeOffset(%d) = %d, want %d", tt.prodMax, got, tt.want)
		}
	}
}

func TestClassifyPKType(t *testing.T) {
	tests := []struct {
		name     string
		types    []string
		defaults []string
		want     string
	}{
		{"serial", []string{"integer"}, []string{"nextval('users_id_seq'::regclass)"}, "serial"},
		{"bigserial", []string{"bigint"}, []string{"nextval('orders_id_seq'::regclass)"}, "bigserial"},
		{"uuid", []string{"uuid"}, []string{""}, "uuid"},
		{"integer no seq", []string{"integer"}, []string{""}, "serial"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyPKType(tt.types, tt.defaults)
			if got != tt.want {
				t.Errorf("classifyPKType(%v, %v) = %q, want %q", tt.types, tt.defaults, got, tt.want)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsSubstr(s, substr)
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func containsFK(s string) bool {
	return contains(s, "FOREIGN KEY")
}
