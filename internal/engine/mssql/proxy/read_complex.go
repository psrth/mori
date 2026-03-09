package proxy

import (
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
)

// handleComplexRead handles CTEs, derived tables, CROSS/OUTER APPLY, and other
// complex read patterns by materializing dirty base tables into temp tables
// and rewriting the query to reference them.
func (rh *ReadHandler) handleComplexRead(
	clientConn net.Conn,
	fullPayload []byte,
	cl *core.Classification,
) error {
	if len(cl.Tables) == 0 {
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	// Identify tables that are "dirty" (have deltas or tombstones or schema diffs).
	var dirtyTables []string
	for _, table := range cl.Tables {
		if rh.isTableDirty(table) {
			dirtyTables = append(dirtyTables, table)
		}
	}

	if len(dirtyTables) == 0 {
		// No dirty tables — safe to forward to Prod.
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	// Materialize each dirty table's data into a temp table.
	rewriteMap := make(map[string]string) // original table -> temp table
	for _, table := range dirtyTables {
		baseSQL := "SELECT * FROM " + quoteIdentMSSQL(table)
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
