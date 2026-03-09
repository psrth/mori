package proxy

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/mori-dev/mori/internal/core"
)

// utilTableName generates a deterministic temp table name for materialization.
func utilTableName(sqlStr string) string {
	h := sha256.Sum256([]byte(sqlStr))
	return fmt.Sprintf("_mori_util_%x", h[:6])
}

// materializeToUtilTable runs a SELECT query as a merged read, then creates
// a temp table on Shadow containing the merged results.
// Returns the temp table name. Caller is responsible for dropping it.
func (p *Proxy) materializeToUtilTable(
	querySQL string,
	cl *core.Classification,
	connID int64,
) (string, error) {
	utilName := utilTableName(querySQL)

	// Execute the base query as a merged read.
	columns, rows, nulls, err := p.mergedReadRows(querySQL, cl, connID)
	if err != nil {
		return "", fmt.Errorf("merged read for materialization: %w", err)
	}

	// Apply max rows cap.
	if p.maxRowsHydrate > 0 && len(rows) > p.maxRowsHydrate {
		rows = rows[:p.maxRowsHydrate]
		nulls = nulls[:p.maxRowsHydrate]
		if p.verbose {
			log.Printf("[conn %d] materialization capped at %d rows", connID, p.maxRowsHydrate)
		}
	}

	// Drop if already exists.
	p.shadowDB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, utilName))

	if len(rows) == 0 {
		// Create empty temp table.
		createSQL := buildCreateTempSQL(utilName, columns)
		if _, err := p.shadowDB.Exec(createSQL); err != nil {
			return "", fmt.Errorf("creating empty util table: %w", err)
		}
		return utilName, nil
	}

	// Create temp table.
	createSQL := buildCreateTempSQL(utilName, columns)
	if _, err := p.shadowDB.Exec(createSQL); err != nil {
		return "", fmt.Errorf("creating util table: %w", err)
	}

	// Insert rows in batches.
	if err := bulkInsertToTemp(p.shadowDB, utilName, columns, rows, nulls); err != nil {
		p.shadowDB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, utilName))
		return "", fmt.Errorf("populating util table: %w", err)
	}

	if p.verbose {
		log.Printf("[conn %d] materialized %d rows into %s", connID, len(rows), utilName)
	}

	return utilName, nil
}

// dropUtilTable drops a utility table from Shadow.
func (p *Proxy) dropUtilTable(utilName string) {
	p.shadowDB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, utilName))
}

// buildCreateTempSQL creates a CREATE TEMP TABLE statement with TEXT columns.
func buildCreateTempSQL(tableName string, columns []string) string {
	var colDefs []string
	for _, col := range columns {
		colDefs = append(colDefs, fmt.Sprintf(`"%s" TEXT`, col))
	}
	return fmt.Sprintf(`CREATE TEMP TABLE IF NOT EXISTS "%s" (%s)`,
		tableName, strings.Join(colDefs, ", "))
}

// bulkInsertToTemp inserts rows into a temp table in batches.
func bulkInsertToTemp(db *sql.DB, tableName string, columns []string, rows [][]sql.NullString, nulls [][]bool) error {
	if len(rows) == 0 {
		return nil
	}

	batchSize := 100
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		batch := rows[start:end]
		batchNulls := nulls[start:end]

		if err := insertBatch(db, tableName, columns, batch, batchNulls); err != nil {
			return err
		}
	}
	return nil
}

func insertBatch(db *sql.DB, tableName string, columns []string, rows [][]sql.NullString, nulls [][]bool) error {
	if len(rows) == 0 {
		return nil
	}

	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = `"` + c + `"`
	}

	var valueTuples []string
	for i, row := range rows {
		vals := make([]string, len(columns))
		for j := range columns {
			if j < len(row) {
				if (i < len(nulls) && j < len(nulls[i]) && nulls[i][j]) || !row[j].Valid {
					vals[j] = "NULL"
				} else {
					vals[j] = "'" + strings.ReplaceAll(row[j].String, "'", "''") + "'"
				}
			} else {
				vals[j] = "NULL"
			}
		}
		valueTuples = append(valueTuples, "("+strings.Join(vals, ", ")+")")
	}

	sql := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES %s`,
		tableName, strings.Join(quotedCols, ", "), strings.Join(valueTuples, ", "))
	_, err := db.Exec(sql)
	return err
}
