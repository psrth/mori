# Mori E2E Test Plan

## Overview

Comprehensive end-to-end test suite for Mori, a PostgreSQL proxy with copy-on-write semantics. Tests validate that the proxy correctly intercepts, classifies, routes, and merges queries against a realistic seed database.

**Seed Database:** 18 tables, ~70k+ rows, covering serial/bigserial/UUID/composite/no-PK types, self-referencing FKs, 1:1/1:N/N:M relationships, and diverse column types (text, int, bigint, uuid, timestamp, boolean, jsonb, numeric, arrays, inet).

**Run:** `go test -tags e2e -v -count=1 -timeout 15m ./e2e/`

---

## Test Categories

### 1. Lifecycle (`01_lifecycle_test.go`)

| Test | Description | Expected |
|------|-------------|----------|
| proxy_accepts_simple_query | SELECT 1 through proxy | Returns 1 |
| proxy_accepts_select_now | SELECT now() through proxy | No error |
| status_shows_initialized | `mori status` output | Shows engine version, PostgreSQL |
| status_shows_no_deltas_initially | Status after fresh init | Delta/tombstone counts are 0 |
| status_shows_table_info | Status output completeness | Shows sequence offsets |
| connection_string_via_proxy | pgx connects normally | current_database() matches |
| multiple_connections_work | Two concurrent pgx conns | Both return results |
| prod_untouched_after_insert | INSERT through proxy | Prod row count unchanged |

### 2. Basic CRUD (`02_basic_crud_test.go`)

#### SELECT (Prod-Direct, Clean State)

| Test | Description | Expected |
|------|-------------|----------|
| select_all_from_small_table | SELECT * FROM roles | 10 rows |
| select_count_star_users | COUNT(*) | 500 |
| select_count_star_orders | COUNT(*) | 2000 |
| select_count_star_audit_log | COUNT(*) high-volume table | 50000 |
| select_with_where_serial_pk | WHERE id = 1 | user_1 |
| select_with_where_bigserial_pk | WHERE id = 1 on orders | 1 row |
| select_with_where_uuid_pk | WHERE slug = 'product-1' | Product 1 |
| select_with_limit | LIMIT 5 | 5 rows |
| select_with_limit_and_offset | LIMIT 10 OFFSET 5 | 10 rows |
| select_with_order_by_asc | ORDER BY id ASC | Ascending IDs |
| select_with_order_by_desc | ORDER BY id DESC | Descending IDs |
| select_distinct | DISTINCT status | Multiple statuses |
| select_with_where_in | WHERE id IN (1,2,3) | 3 rows |
| select_with_where_like | WHERE username LIKE 'user_1%' | >= 1 row |
| select_with_where_between | WHERE id BETWEEN 1 AND 10 | 10 rows |
| select_with_where_is_null | WHERE website IS NULL | >= 1 row |
| select_with_where_is_not_null | WHERE website IS NOT NULL | >= 1 row |

#### INSERT

| Test | Description | Expected |
|------|-------------|----------|
| insert_serial_pk_returns_offset_id | INSERT RETURNING id | ID > 500 (shadow offset) |
| insert_bigserial_pk_into_orders | INSERT RETURNING id | ID > 2000 |
| insert_uuid_pk_products | INSERT RETURNING id | Valid UUID |
| insert_composite_pk_org_members | INSERT composite PK | No error |
| insert_no_pk_table_settings | INSERT into settings | No error |
| insert_with_returning_star | INSERT ... RETURNING * | All columns returned |
| insert_with_null_columns | INSERT with NULL values | No error |
| insert_with_jsonb_value | INSERT nested JSON | No error |
| insert_with_array_value | INSERT text array | No error |
| insert_with_inet_value | INSERT inet type | No error |
| insert_with_numeric_precision | INSERT numeric(10,2) | No error |
| insert_multiple_rows | INSERT 3 rows | 3 rows affected |

#### Merged Reads (after writes trigger MergedRead strategy)

| Test | Description | Expected |
|------|-------------|----------|
| select_after_insert_shows_both | COUNT(*) after inserts | > 500 (prod + shadow) |
| select_with_limit_after_insert | LIMIT query | Correct count |
| select_with_order_by_after_insert | ORDER BY DESC | Shadow rows at top |
| inserted_row_visible_by_value | WHERE username = 'e2e_new_user' | 1 row |

#### UPDATE

| Test | Description | Expected |
|------|-------------|----------|
| update_prod_row_triggers_hydration | UPDATE users SET ... WHERE id=10 | 1 row affected |
| update_prod_row_visible_in_select | SELECT after update | Shows new value |
| update_shadow_row | UPDATE shadow-only row | 1 row affected |
| update_with_returning | UPDATE ... RETURNING | Returns updated row |
| update_jsonb_field | UPDATE metadata || ... | No error |
| update_multiple_columns | UPDATE 3 columns | 1 row affected |
| update_bigserial_pk_row | UPDATE orders | 1 row affected |
| prod_unchanged_after_update | Direct prod SELECT | Original value |

#### DELETE

| Test | Description | Expected |
|------|-------------|----------|
| delete_prod_row_creates_tombstone | DELETE WHERE id=20 | 1 row affected |
| deleted_row_invisible_in_select | SELECT WHERE id=20 | 0 rows |
| delete_with_returning | DELETE ... RETURNING | Returns deleted row |
| delete_preserves_other_rows | SELECT WHERE id=22 | Still exists |
| prod_unchanged_after_delete | Direct prod SELECT id=20 | Still exists |
| tombstone_respects_select_count | COUNT(*) WHERE id=20 | 0 |
| delete_and_select_with_limit | LIMIT query after delete | Tombstoned rows excluded |

#### Full CRUD Cycle

| Test | Description | Expected |
|------|-------------|----------|
| full_crud_cycle | INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT | All steps correct |
| crud_with_timestamps | INSERT/SELECT session with timestamps | Roundtrip works |
| insert_and_verify_not_in_prod | Check prod for e2e rows | 0 rows in prod |

### 3. Complex Queries (`03_complex_queries_test.go`)

#### JOINs

| Test | Description | Expected |
|------|-------------|----------|
| inner_join_users_orders | INNER JOIN users ↔ orders | >= 1 row for user 1 |
| inner_join_after_update | JOIN after UPDATE user 10 | Shows updated display_name |
| inner_join_after_delete | JOIN for tombstoned user 20 | 0 rows |
| left_join_users_posts | LEFT JOIN | >= 1 row |
| three_table_join | orders ↔ order_items ↔ products | >= 1 row |
| self_join_categories | Parent-child categories | >= 5 rows |
| self_join_comments_threaded | Threaded comment replies | >= 1 row |
| join_with_where_clause | JOIN + WHERE filter | >= 1 row |
| join_with_order_by_and_limit | JOIN + ORDER BY + LIMIT | 5 rows |
| cross_join_small_tables | CROSS JOIN roles × tags | 20 rows |

#### Subqueries

| Test | Description | Expected |
|------|-------------|----------|
| subquery_in_where_in | WHERE id IN (SELECT ...) | >= 1 row |
| subquery_in_where_exists | WHERE EXISTS (SELECT ...) | >= 1 row |
| subquery_scalar | Scalar subquery in SELECT | 1 row |
| subquery_in_from_derived_table | Derived table in FROM | >= 1 row |

#### CTEs

| Test | Description | Expected |
|------|-------------|----------|
| simple_cte | WITH ... SELECT | >= 1 row |
| recursive_cte_category_tree | WITH RECURSIVE | 30 categories |
| multiple_ctes | Two CTEs + JOIN | >= 1 row |
| cte_with_join | CTE + JOIN | >= 1 row |

#### Set Operations

| Test | Description | Expected |
|------|-------------|----------|
| union | UNION | >= 3 rows (deduped) |
| union_all | UNION ALL | 2 rows (dupes preserved) |
| intersect | INTERSECT | Logged |
| except | EXCEPT | Logged |

#### Window Functions

| Test | Description | Expected |
|------|-------------|----------|
| row_number_over_partition | ROW_NUMBER() OVER | >= 3 rows |
| rank_over_partition | RANK() OVER | 10 rows |
| lag_lead | LAG/LEAD | 10 rows |
| sum_over_window | SUM() OVER running total | 5 rows |

#### Aggregates & GROUP BY

| Test | Description | Expected |
|------|-------------|----------|
| count_group_by | GROUP BY status | >= 2 groups |
| sum_group_by | SUM(total_amount) | >= 1 row |
| avg_group_by | AVG(total_amount) | No error |
| max_min_group_by | MAX/MIN | 1 group |
| group_by_having | HAVING COUNT(*) > 3 | All counts > 3 |
| count_distinct | COUNT(DISTINCT ...) | >= 2 |

#### Type-Specific Operations

| Test | Description | Expected |
|------|-------------|----------|
| jsonb_arrow_operator | metadata->'theme' | Returns value |
| jsonb_double_arrow_text | metadata->>'lang' | Returns text |
| jsonb_contains_operator | metadata @> '{"theme":"dark"}' | >= 1 row |
| array_contains_any | 'admin' = ANY(tags) | >= 1 row |
| string_like | LIKE pattern | >= 1 row |
| string_ilike | ILIKE pattern | >= 1 row |
| date_extract | EXTRACT(YEAR FROM ...) | No error |
| date_trunc | DATE_TRUNC('day', ...) | No error |
| case_expression | CASE WHEN ... | 5 rows |
| coalesce | COALESCE(website, ...) | 5 rows |

### 4. Migrations (`04_migrations_test.go`)

| Test | Description | Expected |
|------|-------------|----------|
| add_column_text | ALTER TABLE ADD COLUMN phone TEXT | No error |
| select_after_add_column_shows_null | SELECT phone from prod row | NULL |
| insert_after_add_column | INSERT with phone value | No error |
| select_new_column_has_value | SELECT phone from shadow row | '555-0199' |
| add_column_with_default | ADD COLUMN priority INTEGER DEFAULT 0 | No error |
| add_column_boolean | ADD COLUMN is_expedited BOOLEAN | No error |
| drop_column | DROP COLUMN avatar_url | No error |
| select_after_drop_column | SELECT * | avatar_url absent |
| rename_column | RENAME COLUMN bio TO biography | No error |
| select_after_rename | SELECT biography | Column exists |
| alter_column_type | ALTER COLUMN TYPE BIGINT | No error |
| create_new_table | CREATE TABLE | No error |
| insert_into_new_table | INSERT into new table | No error |
| select_from_new_table | SELECT from new table | 2 rows |
| create_index | CREATE INDEX | No error |
| drop_table | DROP TABLE | No error |
| select_from_dropped_table | SELECT from dropped table | Error |
| update_after_add_column | UPDATE phone column | Correct value |
| join_after_ddl | JOIN after schema changes | >= 1 row |
| schema_status_shows_diffs | mori status | Shows diffs |

### 5. Edge Cases (`05_edge_cases_test.go`)

#### No-PK Table

| Test | Description | Expected |
|------|-------------|----------|
| select_from_settings | SELECT all settings | >= 20 rows |
| insert_into_settings | INSERT new setting | No error |
| select_settings_after_insert | SELECT inserted setting | 1 row |

#### UUID PK

| Test | Description | Expected |
|------|-------------|----------|
| select_products_by_slug | SELECT by slug | 1 row |
| insert_uuid_product | INSERT RETURNING UUID | Valid UUID |
| select_uuid_product_after_insert | SELECT inserted product | 1 row |
| uuid_payments_select | SELECT from payments | 5 rows |
| uuid_sessions_select | SELECT from sessions | 5 rows |

#### Composite PK

| Test | Description | Expected |
|------|-------------|----------|
| select_org_members | SELECT with org_id filter | >= 1 row |
| insert_composite_pk | INSERT into post_tags | No error |
| select_user_roles | SELECT user_roles | >= 1 row |

#### NULL Handling

| Test | Description | Expected |
|------|-------------|----------|
| insert_all_nullable_null | INSERT with NULLs | No error |
| select_where_null | WHERE IS NULL | 1 row |
| update_set_to_null | SET col = NULL | NULL in result |
| update_null_to_value | SET col = 'value' | Value in result |

#### Large Text

| Test | Description | Expected |
|------|-------------|----------|
| insert_10kb_text | 10KB body | No error |
| read_10kb_text_roundtrip | Read 10KB back | Exact match |
| insert_100kb_text | 100KB body | No error |
| read_100kb_text_roundtrip | Read 100KB back | Exact match |

#### Empty Results

| Test | Description | Expected |
|------|-------------|----------|
| select_impossible_where | WHERE 1=0 | 0 rows |
| select_nonexistent_id | WHERE id=999999 | 0 rows |

#### Concurrent Connections

| Test | Description | Expected |
|------|-------------|----------|
| two_connections_see_inserts | Insert via conn1, read via conn2 | Visible |
| parallel_inserts | 5 goroutines insert simultaneously | All succeed |

#### Large Result Set

| Test | Description | Expected |
|------|-------------|----------|
| select_50k_audit_log | COUNT(*) | >= 50000 |
| select_large_limit | LIMIT 1000 | 1000 rows |

#### Parameterized Queries (Extended Protocol)

| Test | Description | Expected |
|------|-------------|----------|
| parameterized_select_single | SELECT WHERE id=$1 | Correct row |
| parameterized_select_multiple | SELECT WHERE id>=$1 AND id<=$2 | 5 rows |
| parameterized_insert | INSERT VALUES ($1, $2) | 1 row affected |
| parameterized_update | UPDATE SET $1 WHERE $2 | 1 row affected |
| parameterized_delete | DELETE WHERE $1 | 1 row affected |
| parameterized_null_param | UPDATE SET NULL | 1 row affected |
| prepared_statement_reuse | Prepare + Execute twice | Both correct |
| parameterized_after_insert | INSERT then SELECT params | Visible |

#### Transactions

| Test | Description | Expected |
|------|-------------|----------|
| begin_commit_visible | BEGIN; INSERT; COMMIT | Row visible |
| begin_rollback_invisible | BEGIN; INSERT; ROLLBACK | Row NOT visible |
| transaction_mixed_read_write | Mixed SELECT/INSERT in txn | All succeed |
| transaction_with_params | Parameterized INSERT in txn | Success |

#### Special Characters

| Test | Description | Expected |
|------|-------------|----------|
| single_quotes | O'Brien | Roundtrip correct |
| unicode | Emoji/CJK characters | Roundtrip correct |
| newlines | Multi-line text | Roundtrip correct |

#### Known Limitations (Documented Behavior)

| Test | Description | Expected |
|------|-------------|----------|
| multi_column_order_by | ORDER BY col1, col2 | Executes (may not re-sort) |
| aggregate_on_merged_table | COUNT(*) on modified table | Returns count (may differ) |

### 6. MCP Server (`06_mcp_test.go`)

| Test | Description | Expected |
|------|-------------|----------|
| select_query_returns_rows | SELECT via MCP | JSON with row count |
| select_with_where | SELECT WHERE id=1 via MCP | Contains user_1 |
| insert_via_mcp | INSERT via MCP | OK response |
| insert_then_select_via_mcp | INSERT + SELECT via MCP | Inserted row visible |
| update_via_mcp | UPDATE via MCP | OK response |
| delete_via_mcp | DELETE via MCP | OK response |
| ddl_via_mcp | CREATE TABLE + INSERT via MCP | Table created, row visible |
| empty_query_error | Empty string query | Error |
| invalid_sql_error | Malformed SQL | Error |
| nonexistent_table_error | SELECT from missing table | Error |

---

## Known v1 Limitations

These are documented behaviors, not bugs:

1. **Multi-column ORDER BY**: Only single-column sort is re-applied in Go after merge. Multi-column ORDER BY results may not be perfectly sorted.
2. **Parameterized LIMIT**: `LIMIT $1` is not rewritten for over-fetching in merged reads.
3. **Aggregate merging**: `COUNT(*)`, `SUM()` etc. on tables with deltas run independently on both backends; results are merged as rows, not as aggregated values.
4. **JOIN PK patching**: Requires PK column in SELECT list. Without PK, patching is skipped.
5. **WHERE not re-evaluated**: After patching JOIN rows with shadow values, the WHERE clause is not re-checked.
6. **SAVEPOINT**: Forwarded but not tracked.

---

## Seed Database Summary

| Table | PK Type | Rows | Key Relationships |
|-------|---------|------|-------------------|
| users | serial | 500 | Base entity |
| user_profiles | serial | 500 | 1:1 → users |
| organizations | serial | 50 | Independent |
| org_members | composite | ~1000 | N:M users ↔ orgs |
| roles | serial | 10 | Lookup table |
| user_roles | composite | ~1500 | N:M users ↔ roles |
| products | UUID | 200 | Deterministic UUIDs |
| categories | serial | 30 | Self-ref FK (3 levels) |
| orders | bigserial | 2000 | FK → users |
| order_items | serial | 5000 | FK → orders + products |
| payments | UUID | 1500 | FK → orders |
| posts | serial | 1000 | FK → users |
| comments | serial | 3000 | Self-ref FK, FK → posts, users |
| tags | serial | 50 | Unique names |
| post_tags | composite | ~3000 | N:M posts ↔ tags |
| audit_log | bigserial | 50000 | High volume |
| settings | NO PK | 20 | Edge case |
| sessions | UUID | 1000 | FK → users |
