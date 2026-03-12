package proxy

import (
	"log"
	"net"

	"github.com/psrth/mori/internal/core"
)

// handleComplexRead handles CTEs, derived tables, CROSS/OUTER APPLY, and other
// complex read patterns by materializing ALL base tables into temp tables
// and rewriting the query to reference them. Even clean tables must be
// materialized because the rewritten query runs on shadow, where clean
// tables have no data.
//
// Also collects tables from correlated subqueries (e.g., in SELECT list)
// since these tables may not appear in cl.Tables.
func (rh *ReadHandler) handleComplexRead(
	clientConn net.Conn,
	fullPayload []byte,
	cl *core.Classification,
) error {
	if len(cl.Tables) == 0 {
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	// Check if any table is dirty.
	hasDirty := false
	for _, table := range cl.Tables {
		if rh.isTableDirty(table) {
			hasDirty = true
			break
		}
	}

	if !hasDirty {
		// No dirty tables — safe to forward to Prod.
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	// Collect ALL tables including those in subqueries.
	allTables := cl.Tables
	subqueryTables := collectSubqueryTablesFromSQL(cl.RawSQL)
	for _, t := range subqueryTables {
		found := false
		for _, existing := range allTables {
			if existing == t {
				found = true
				break
			}
		}
		if !found {
			allTables = append(allTables, t)
		}
	}

	// Materialize ALL tables' data into temp tables.
	rewriteMap := make(map[string]string) // original table -> temp table
	for _, table := range allTables {
		baseSQL := "SELECT * FROM " + quoteIdentMSSQL(table)
		baseSQL = rh.capSQL(baseSQL)
		baseCl := &core.Classification{
			OpType:  core.OpRead,
			SubType: core.SubSelect,
			RawSQL:  baseSQL,
			Tables:  []string{table},
		}

		columns, values, nulls, err := rh.mergedReadCore(baseCl, baseSQL)
		if err != nil {
			if rh.verbose {
				log.Printf("[conn %d] complex read: merge failed for %s: %v", rh.connID, table, err)
			}
			// Clean up already-created temp tables.
			for _, tempName := range rewriteMap {
				dropUtilTable(rh.shadowConn, tempName)
			}
			return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
		}

		tempName, err := materializeToTempTable(rh.shadowConn, "complex_"+table, columns, values, nulls)
		if err != nil {
			if rh.verbose {
				log.Printf("[conn %d] complex read: materialize failed for %s: %v", rh.connID, table, err)
			}
			for _, tn := range rewriteMap {
				dropUtilTable(rh.shadowConn, tn)
			}
			return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
		}
		rewriteMap[table] = tempName
	}

	// Clean up temp tables when done.
	defer func() {
		for _, tempName := range rewriteMap {
			dropUtilTable(rh.shadowConn, tempName)
		}
	}()

	// Rewrite the query to reference temp tables.
	rewrittenSQL := cl.RawSQL
	for original, temp := range rewriteMap {
		rewrittenSQL = rewriteTableReference(rewrittenSQL, original, temp)
	}

	// Execute on Shadow.
	allHeaders := extractAllHeaders(fullPayload)
	var batchMsg []byte
	if allHeaders != nil {
		batchMsg = buildSQLBatchWithHeaders(allHeaders, rewrittenSQL)
	} else {
		batchMsg = buildSQLBatchMessage(rewrittenSQL)
	}
	return forwardAndRelay(batchMsg, rh.shadowConn, clientConn)
}

// isTableDirty checks if a table has any deltas, tombstones, or schema diffs.
func (rh *ReadHandler) isTableDirty(table string) bool {
	if rh.deltaMap.CountForTable(table) > 0 {
		return true
	}
	if rh.tombstones.CountForTable(table) > 0 {
		return true
	}
	if rh.schemaRegistry != nil && rh.schemaRegistry.HasDiff(table) {
		return true
	}
	return false
}
