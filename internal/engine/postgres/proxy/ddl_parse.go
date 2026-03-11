package proxy

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type ddlChangeKind int

const (
	ddlAddColumn    ddlChangeKind = iota
	ddlDropColumn
	ddlRenameColumn
	ddlAlterType
	ddlDropTable
	ddlCreateTable
	ddlRenameTable
)

type ddlChange struct {
	Kind    ddlChangeKind
	Table   string
	Column  string
	ColType string
	OldName string
	NewName string
	OldType string
	NewType string
	Default *string
}

// parseDDLChanges parses a DDL SQL statement and returns structured schema changes.
// Returns nil, nil for DDL that does not affect column-level schema (CREATE TABLE,
// CREATE INDEX, ADD CONSTRAINT, etc.).
func parseDDLChanges(sql string) ([]ddlChange, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse DDL: %w", err)
	}

	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return nil, nil
	}

	node := stmts[0].GetStmt()
	if node == nil {
		return nil, nil
	}

	switch {
	case node.GetAlterTableStmt() != nil:
		return parseAlterTable(node.GetAlterTableStmt())
	case node.GetRenameStmt() != nil:
		return parseRenameStmt(node.GetRenameStmt())
	case node.GetDropStmt() != nil:
		return parseDropStmt(node.GetDropStmt())
	case node.GetCreateStmt() != nil:
		table := rangeVarName(node.GetCreateStmt().GetRelation())
		if table != "" {
			return []ddlChange{{Kind: ddlCreateTable, Table: table}}, nil
		}
		return nil, nil
	default:
		// CREATE INDEX, etc. — no column-level changes to track.
		return nil, nil
	}
}

// parseAlterTable extracts column-level changes from an ALTER TABLE statement.
func parseAlterTable(stmt *pg_query.AlterTableStmt) ([]ddlChange, error) {
	table := rangeVarName(stmt.GetRelation())
	if table == "" {
		return nil, fmt.Errorf("ALTER TABLE: missing table name")
	}

	var changes []ddlChange
	for _, cmdNode := range stmt.GetCmds() {
		cmd := cmdNode.GetAlterTableCmd()
		if cmd == nil {
			continue
		}

		switch cmd.GetSubtype() {
		case pg_query.AlterTableType_AT_AddColumn:
			ch, err := parseAddColumn(table, cmd)
			if err != nil {
				return nil, err
			}
			if ch != nil {
				changes = append(changes, *ch)
			}

		case pg_query.AlterTableType_AT_DropColumn:
			colName := cmd.GetName()
			if colName != "" {
				changes = append(changes, ddlChange{
					Kind:   ddlDropColumn,
					Table:  table,
					Column: colName,
				})
			}

		case pg_query.AlterTableType_AT_AlterColumnType:
			ch, err := parseAlterColumnType(table, cmd)
			if err != nil {
				return nil, err
			}
			if ch != nil {
				changes = append(changes, *ch)
			}

		default:
			// Constraints, indexes, ownership, etc. — not column-level schema.
		}
	}

	return changes, nil
}

// parseAddColumn extracts column name, type, and optional default from an ADD COLUMN command.
func parseAddColumn(table string, cmd *pg_query.AlterTableCmd) (*ddlChange, error) {
	defNode := cmd.GetDef()
	if defNode == nil {
		return nil, nil
	}
	colDef := defNode.GetColumnDef()
	if colDef == nil {
		return nil, nil
	}

	colName := colDef.GetColname()
	colType := typeNameToString(colDef.GetTypeName())

	ch := &ddlChange{
		Kind:    ddlAddColumn,
		Table:   table,
		Column:  colName,
		ColType: colType,
	}

	// DEFAULT may be in RawDefault or in constraints (contype=CONSTR_DEFAULT=3).
	if rawDefault := colDef.GetRawDefault(); rawDefault != nil {
		defStr := deparseDefault(rawDefault)
		if defStr != "" {
			ch.Default = &defStr
		}
	}
	for _, c := range colDef.GetConstraints() {
		if con := c.GetConstraint(); con != nil && con.GetContype() == 3 {
			if rawExpr := con.GetRawExpr(); rawExpr != nil {
				defStr := deparseDefault(rawExpr)
				if defStr != "" {
					ch.Default = &defStr
				}
			}
		}
	}

	return ch, nil
}

// parseAlterColumnType extracts column name and new type from an ALTER COLUMN TYPE command.
func parseAlterColumnType(table string, cmd *pg_query.AlterTableCmd) (*ddlChange, error) {
	colName := cmd.GetName()
	if colName == "" {
		return nil, nil
	}

	defNode := cmd.GetDef()
	if defNode == nil {
		return nil, nil
	}
	colDef := defNode.GetColumnDef()
	if colDef == nil {
		return nil, nil
	}

	newType := typeNameToString(colDef.GetTypeName())

	return &ddlChange{
		Kind:    ddlAlterType,
		Table:   table,
		Column:  colName,
		OldType: "unknown",
		NewType: newType,
	}, nil
}

// parseRenameStmt extracts column and table rename changes. Handles column
// renames (ALTER TABLE ... RENAME COLUMN) and table renames (ALTER TABLE ...
// RENAME TO). Other rename types (index, etc.) are ignored.
func parseRenameStmt(stmt *pg_query.RenameStmt) ([]ddlChange, error) {
	switch stmt.GetRenameType() {
	case pg_query.ObjectType_OBJECT_COLUMN:
		table := rangeVarName(stmt.GetRelation())
		if table == "" {
			return nil, nil
		}
		return []ddlChange{{
			Kind:    ddlRenameColumn,
			Table:   table,
			OldName: stmt.GetSubname(),
			NewName: stmt.GetNewname(),
		}}, nil

	case pg_query.ObjectType_OBJECT_TABLE:
		oldName := rangeVarName(stmt.GetRelation())
		if oldName == "" {
			return nil, nil
		}
		return []ddlChange{{
			Kind:    ddlRenameTable,
			OldName: oldName,
			NewName: stmt.GetNewname(),
		}}, nil

	default:
		return nil, nil
	}
}

// parseDropStmt extracts table drop changes. Only handles DROP TABLE;
// other DROP types are ignored.
func parseDropStmt(stmt *pg_query.DropStmt) ([]ddlChange, error) {
	if stmt.GetRemoveType() != pg_query.ObjectType_OBJECT_TABLE {
		return nil, nil
	}

	var changes []ddlChange
	for _, obj := range stmt.GetObjects() {
		if list := obj.GetList(); list != nil {
			var parts []string
			for _, item := range list.GetItems() {
				if s := item.GetString_(); s != nil {
					parts = append(parts, s.GetSval())
				}
			}
			if len(parts) > 0 {
				changes = append(changes, ddlChange{
					Kind:  ddlDropTable,
					Table: strings.Join(parts, "."),
				})
			}
		}
	}

	return changes, nil
}

// typeNameToString converts a pg_query TypeName to a SQL type string.
func typeNameToString(tn *pg_query.TypeName) string {
	if tn == nil {
		return "unknown"
	}

	var typeName string
	for _, n := range tn.GetNames() {
		if s := n.GetString_(); s != nil {
			val := s.GetSval()
			if val == "pg_catalog" {
				continue
			}
			typeName = val
		}
	}

	typeName = pgTypeToSQL(typeName)

	// Append type modifiers (e.g., VARCHAR(255), NUMERIC(10,2)).
	if mods := tn.GetTypmods(); len(mods) > 0 {
		var modStrs []string
		for _, m := range mods {
			if ac := m.GetAConst(); ac != nil {
				if iv := ac.GetIval(); iv != nil {
					modStrs = append(modStrs, fmt.Sprintf("%d", iv.GetIval()))
				}
			} else if ic := m.GetInteger(); ic != nil {
				modStrs = append(modStrs, fmt.Sprintf("%d", ic.GetIval()))
			}
		}
		if len(modStrs) > 0 {
			typeName += "(" + strings.Join(modStrs, ",") + ")"
		}
	}

	return typeName
}

// pgTypeToSQL maps PostgreSQL internal type names to their SQL equivalents.
func pgTypeToSQL(name string) string {
	switch name {
	case "int2":
		return "SMALLINT"
	case "int4":
		return "INTEGER"
	case "int8":
		return "BIGINT"
	case "float4":
		return "REAL"
	case "float8":
		return "DOUBLE PRECISION"
	case "bool":
		return "BOOLEAN"
	case "varchar":
		return "VARCHAR"
	case "bpchar":
		return "CHAR"
	case "numeric":
		return "NUMERIC"
	default:
		return strings.ToUpper(name)
	}
}

// rangeVarName extracts a table name from a RangeVar, with optional schema prefix.
func rangeVarName(rv *pg_query.RangeVar) string {
	if rv == nil {
		return ""
	}
	name := rv.GetRelname()
	if s := rv.GetSchemaname(); s != "" {
		name = s + "." + name
	}
	return name
}

// deparseDefault attempts to extract a simple default value from an expression node.
// Handles string constants and integer constants. Returns "" for complex expressions.
func deparseDefault(node *pg_query.Node) string {
	if node == nil {
		return ""
	}

	// String constant (e.g., DEFAULT 'active').
	if ac := node.GetAConst(); ac != nil {
		if sv := ac.GetSval(); sv != nil {
			return sv.GetSval()
		}
		if iv := ac.GetIval(); iv != nil {
			return fmt.Sprintf("%d", iv.GetIval())
		}
		if fv := ac.GetFval(); fv != nil {
			return fv.GetFval()
		}
		if ac.GetBoolval() != nil {
			return fmt.Sprintf("%t", ac.GetBoolval().GetBoolval())
		}
	}

	// Type cast (e.g., DEFAULT 'active'::text) — extract the inner constant.
	if tc := node.GetTypeCast(); tc != nil {
		return deparseDefault(tc.GetArg())
	}

	return ""
}
