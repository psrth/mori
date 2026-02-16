package proxy

import (
	"fmt"
	"log"
	"net"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
)

// handleUpdate handles UPDATE statements with hydration from Prod when needed.
//
// Point update (PKs extractable): for each (table, pk), hydrate from Prod if
// the row is not already in Shadow, then execute the UPDATE on Shadow and
// track the affected PKs in the delta map.
//
// Bulk update (no PKs): query Prod for matching rows, hydrate them into
// Shadow, then execute the UPDATE on Shadow and track all affected PKs.
func (w *WriteHandler) handleUpdate(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	if len(cl.PKs) == 0 {
		// Attempt bulk hydration for single-table UPDATEs with PK metadata.
		if !cl.IsJoin && len(cl.Tables) == 1 {
			table := cl.Tables[0]
			if meta, ok := w.tables[table]; ok && len(meta.PKColumns) > 0 {
				return w.handleBulkUpdate(clientConn, rawMsg, cl)
			}
		}
		// Fallback: multi-table UPDATE or no PK metadata.
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE without PKs, forwarding to Shadow without hydration", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Point update: hydrate missing rows, then execute.
	for _, pk := range cl.PKs {
		if w.deltaMap.IsDelta(pk.Table, pk.PK) {
			continue // Already in Shadow.
		}
		if err := w.hydrateRow(pk.Table, pk.PK); err != nil {
			if w.verbose {
				log.Printf("[conn %d] hydration failed for (%s, %s): %v", w.connID, pk.Table, pk.PK, err)
			}
			// Non-fatal: the UPDATE may still succeed if the row exists
			// in Shadow or doesn't exist at all.
		}
	}

	// Execute the UPDATE on Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Update delta map for all PKs.
	for _, pk := range cl.PKs {
		if w.inTxn() {
			w.deltaMap.Stage(pk.Table, pk.PK)
		} else {
			w.deltaMap.Add(pk.Table, pk.PK)
		}
	}

	// Only persist immediately in autocommit mode; txn commit handles persistence.
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// handleBulkUpdate handles UPDATE statements where no PKs could be extracted
// from the WHERE clause (e.g., range conditions, IS NULL, complex expressions).
//
// It queries Prod for all rows matching the WHERE clause, hydrates them into
// Shadow, executes the UPDATE on Shadow, and tracks all affected PKs.
func (w *WriteHandler) handleBulkUpdate(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	table := cl.Tables[0]
	meta := w.tables[table]
	pkCol := meta.PKColumns[0]

	// 1. Build a SELECT query to discover matching rows from Prod.
	selectSQL, err := buildBulkHydrationQuery(cl.RawSQL)
	if err != nil {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: failed to build hydration query: %v", w.connID, err)
		}
		// Fallback: forward to Shadow without hydration.
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	if w.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrating from Prod with: %s", w.connID, selectSQL)
	}

	// 2. Execute on Prod to get all matching rows.
	result, err := execQuery(w.prodConn, selectSQL)
	if err != nil {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: Prod query failed: %v", w.connID, err)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}
	if result.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: Prod query error: %s", w.connID, result.Error)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// 3. Find PK column index in results.
	pkIdx := -1
	for i, col := range result.Columns {
		if col.Name == pkCol {
			pkIdx = i
			break
		}
	}
	if pkIdx == -1 {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: PK column %q not found in Prod results", w.connID, pkCol)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// 4. Hydrate each matching row into Shadow.
	var affectedPKs []string
	hydratedCount := 0
	for i, row := range result.RowValues {
		if result.RowNulls[i][pkIdx] {
			continue // NULL PK, skip.
		}
		pk := row[pkIdx]
		affectedPKs = append(affectedPKs, pk)

		if w.deltaMap.IsDelta(table, pk) {
			continue // Already in Shadow.
		}

		insertSQL := buildInsertSQL(table, result.Columns, row, result.RowNulls[i])
		shadowResult, err := execQuery(w.shadowConn, insertSQL)
		if err != nil {
			if w.verbose {
				log.Printf("[conn %d] bulk UPDATE: hydration INSERT failed for PK %s: %v", w.connID, pk, err)
			}
			continue
		}
		if shadowResult.Error != "" && w.verbose {
			log.Printf("[conn %d] bulk UPDATE: hydration INSERT for PK %s: %s", w.connID, pk, shadowResult.Error)
		}
		hydratedCount++
	}

	if w.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrated %d rows (%d total matched)", w.connID, hydratedCount, len(affectedPKs))
	}

	// 5. Execute the original UPDATE on Shadow.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// 6. Track all affected PKs in the delta map.
	for _, pk := range affectedPKs {
		if w.inTxn() {
			w.deltaMap.Stage(table, pk)
		} else {
			w.deltaMap.Add(table, pk)
		}
	}

	// 7. Persist delta map (autocommit only).
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// buildBulkHydrationQuery transforms an UPDATE statement into a SELECT *
// query with the same FROM and WHERE clauses. This allows querying Prod
// for all rows that the UPDATE would affect.
//
// Example:
//
//	UPDATE conversations SET org_id = 1 WHERE org_id IS NULL
//	→ SELECT * FROM conversations WHERE org_id IS NULL
func buildBulkHydrationQuery(rawSQL string) (string, error) {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}

	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return "", fmt.Errorf("no statements found")
	}

	upd := stmts[0].GetStmt().GetUpdateStmt()
	if upd == nil {
		return "", fmt.Errorf("not an UPDATE statement")
	}

	rel := upd.GetRelation()
	if rel == nil {
		return "", fmt.Errorf("UPDATE has no target relation")
	}

	// Build SELECT * target list.
	starTarget := &pg_query.Node{
		Node: &pg_query.Node_ResTarget{
			ResTarget: &pg_query.ResTarget{
				Val: &pg_query.Node{
					Node: &pg_query.Node_ColumnRef{
						ColumnRef: &pg_query.ColumnRef{
							Fields: []*pg_query.Node{
								{Node: &pg_query.Node_AStar{AStar: &pg_query.A_Star{}}},
							},
						},
					},
				},
			},
		},
	}

	// Build FROM clause: target table + any additional FROM tables.
	fromClause := []*pg_query.Node{
		{Node: &pg_query.Node_RangeVar{RangeVar: rel}},
	}
	fromClause = append(fromClause, upd.GetFromClause()...)

	// Build SELECT statement reusing the WHERE clause.
	selectStmt := &pg_query.SelectStmt{
		TargetList:  []*pg_query.Node{starTarget},
		FromClause:  fromClause,
		WhereClause: upd.GetWhereClause(),
	}

	deparseResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{
				Stmt: &pg_query.Node{
					Node: &pg_query.Node_SelectStmt{
						SelectStmt: selectStmt,
					},
				},
			},
		},
	}

	sql, err := pg_query.Deparse(deparseResult)
	if err != nil {
		return "", fmt.Errorf("deparse: %w", err)
	}

	return sql, nil
}

// hydrateRow fetches a single row from Prod by PK and inserts it into Shadow.
// If the row does not exist in Prod, it is a no-op.
func (w *WriteHandler) hydrateRow(table, pk string) error {
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return fmt.Errorf("no PK metadata for table %q", table)
	}

	// Build SELECT query using the first PK column.
	// For composite PKs, a future phase will handle all columns.
	pkCol := meta.PKColumns[0]
	selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s",
		quoteIdent(table), quoteIdent(pkCol), quoteLiteral(pk))

	result, err := execQuery(w.prodConn, selectSQL)
	if err != nil {
		return fmt.Errorf("prod SELECT: %w", err)
	}
	if result.Error != "" {
		return fmt.Errorf("prod SELECT error: %s", result.Error)
	}
	if len(result.RowValues) == 0 {
		return nil // Row doesn't exist in Prod; nothing to hydrate.
	}

	// Build and execute INSERT into Shadow.
	insertSQL := buildInsertSQL(table, result.Columns, result.RowValues[0], result.RowNulls[0])

	shadowResult, err := execQuery(w.shadowConn, insertSQL)
	if err != nil {
		return fmt.Errorf("shadow INSERT: %w", err)
	}
	if shadowResult.Error != "" {
		// Could be a constraint violation from concurrent hydration.
		// ON CONFLICT DO NOTHING handles this; log and continue.
		if w.verbose {
			log.Printf("[conn %d] hydration INSERT for (%s, %s): %s", w.connID, table, pk, shadowResult.Error)
		}
	}

	return nil
}
