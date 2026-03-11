package proxy

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/psrth/mori/internal/core"
)

// utilTableName generates a deterministic temp table name for materialization.
// Format: _mori_util_{first 12 hex chars of sha256}
func utilTableName(sqlStr string) string {
	h := sha256.Sum256([]byte(sqlStr))
	return fmt.Sprintf("_mori_util_%x", h[:6])
}

// materializeToUtilTable runs a merged read and creates a temp table on shadow
// containing the merged results. Returns the temp table name.
// Caller is responsible for dropping it after use.
func (p *Proxy) materializeToUtilTable(sqlStr string, table string, connID int64, maxRows int) (string, error) {
	utilName := utilTableName(sqlStr)

	// Execute the base query as a merged read.
	baseCl := &core_classification_stub{tables: []string{table}}
	columns, values, nulls, err := p.mergedReadRows(sqlStr, baseCl.toCoreClassification(), connID)
	if err != nil {
		return "", fmt.Errorf("merged read for materialization: %w", err)
	}

	// Apply max rows cap.
	if maxRows > 0 && len(values) > maxRows {
		values = values[:maxRows]
		nulls = nulls[:maxRows]
		if p.verbose {
			log.Printf("[conn %d] materialization capped at %d rows (max_rows_hydrate)", connID, maxRows)
		}
	}

	// Create the temp table on shadow.
	createSQL := buildCreateTempTableFromCols(utilName, columns)
	if _, err := p.shadowDB.Exec(createSQL); err != nil {
		// Table might already exist — drop and recreate.
		p.shadowDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %q", utilName))
		if _, err := p.shadowDB.Exec(createSQL); err != nil {
			return "", fmt.Errorf("creating util table: %w", err)
		}
	}

	if len(values) == 0 {
		return utilName, nil
	}

	// Insert merged rows into the temp table in batches.
	if err := bulkInsertToUtilTableSQLite(p.shadowDB, utilName, columns, values, nulls); err != nil {
		p.shadowDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %q", utilName))
		return "", fmt.Errorf("populating util table: %w", err)
	}

	if p.verbose {
		log.Printf("[conn %d] materialized %d rows into %s", connID, len(values), utilName)
	}

	return utilName, nil
}

// dropUtilTable drops a utility temp table on shadow.
func (p *Proxy) dropUtilTable(utilName string) {
	p.shadowDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %q", utilName))
}

// buildCreateTempTableFromCols builds a CREATE TEMP TABLE statement from column names.
// All columns use TEXT type since SQLite is dynamically typed.
func buildCreateTempTableFromCols(tableName string, columns []string) string {
	var colDefs []string
	for _, col := range columns {
		colDefs = append(colDefs, fmt.Sprintf("%q TEXT", col))
	}
	return fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %q (%s)",
		tableName, strings.Join(colDefs, ", "))
}

// bulkInsertToUtilTableSQLite inserts rows into a utility table using multi-row INSERT.
func bulkInsertToUtilTableSQLite(
	db *sql.DB,
	tableName string,
	columns []string,
	values [][]sql.NullString,
	nulls [][]bool,
) error {
	if len(values) == 0 {
		return nil
	}

	var colNames []string
	for _, col := range columns {
		colNames = append(colNames, fmt.Sprintf("%q", col))
	}
	colList := strings.Join(colNames, ", ")

	// Insert in batches of 100 rows.
	const batchSize = 100
	for i := 0; i < len(values); i += batchSize {
		end := min(i+batchSize, len(values))

		var rowStrings []string
		for ri, row := range values[i:end] {
			rowIdx := i + ri
			var vals []string
			for j, v := range row {
				if j < len(nulls[rowIdx]) && nulls[rowIdx][j] {
					vals = append(vals, "NULL")
				} else if !v.Valid {
					vals = append(vals, "NULL")
				} else {
					vals = append(vals, "'"+strings.ReplaceAll(v.String, "'", "''")+"'")
				}
			}
			rowStrings = append(rowStrings, "("+strings.Join(vals, ", ")+")")
		}

		insertSQL := fmt.Sprintf("INSERT INTO %q (%s) VALUES %s",
			tableName, colList, strings.Join(rowStrings, ", "))

		if _, err := db.Exec(insertSQL); err != nil {
			return fmt.Errorf("batch insert: %w", err)
		}
	}

	return nil
}

// core_classification_stub is a helper to create a minimal Classification for mergedReadRows.
type core_classification_stub struct {
	tables []string
}

func (s *core_classification_stub) toCoreClassification() *core.Classification {
	return &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  s.tables,
	}
}
