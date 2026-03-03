package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// DDLHandler handles DDL operations for a single connection.
// It executes DDL on Shadow, parses the changes, and updates the schema registry.
type DDLHandler struct {
	shadowConn     net.Conn
	schemaRegistry *coreSchema.Registry
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	moriDir        string
	connID         int64
	verbose        bool
	logger         *logging.Logger
}

// HandleDDL executes a DDL statement on Shadow and updates the schema registry.
// The response is relayed to the client. If the DDL fails on Shadow, the error
// is relayed to the client and the registry is not updated.
func (dh *DDLHandler) HandleDDL(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Strip FK constraints before sending to Shadow — Shadow can't validate
	// foreign keys against Prod rows.
	ddlMsg := rawMsg
	if strippedSQL, fkTables := stripFKConstraints(cl.RawSQL); fkTables != nil {
		log.Printf("[conn %d] DDL: stripping REFERENCES constraints (tables: %s) — FK validation deferred to production migration",
			dh.connID, strings.Join(fkTables, ", "))
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("FK constraints stripped (refs: %s)", strings.Join(fkTables, ", ")))
		ddlMsg = buildQueryMsg(strippedSQL)
	}

	// Execute DDL on Shadow, relay response to client, and detect errors.
	hadError, err := forwardAndRelayDDL(ddlMsg, dh.shadowConn, clientConn)
	if err != nil {
		return fmt.Errorf("DDL forward: %w", err)
	}

	// If DDL failed on Shadow, skip registry update.
	if hadError {
		if dh.verbose {
			log.Printf("[conn %d] DDL failed on Shadow, skipping registry update", dh.connID)
		}
		return nil
	}

	if dh.schemaRegistry == nil {
		return nil
	}

	// Parse DDL to extract schema changes.
	changes, err := parseDDLChanges(cl.RawSQL)
	if err != nil {
		if dh.verbose {
			log.Printf("[conn %d] DDL parse warning (registry not updated): %v", dh.connID, err)
		}
		return nil // Non-fatal: DDL succeeded, we just can't track the change.
	}

	if len(changes) == 0 {
		return nil
	}

	// Apply changes to the schema registry.
	for _, ch := range changes {
		dh.applyChange(ch)
	}

	// Persist the registry.
	if err := coreSchema.WriteRegistry(dh.moriDir, dh.schemaRegistry); err != nil {
		if dh.verbose {
			log.Printf("[conn %d] failed to persist schema registry: %v", dh.connID, err)
		}
	}

	return nil
}

// applyChange records a single schema change in the registry.
func (dh *DDLHandler) applyChange(ch ddlChange) {
	switch ch.Kind {
	case ddlAddColumn:
		col := coreSchema.Column{
			Name:    ch.Column,
			Type:    ch.ColType,
			Default: ch.Default,
		}
		dh.schemaRegistry.RecordAddColumn(ch.Table, col)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: ADD COLUMN %s.%s (%s)", dh.connID, ch.Table, ch.Column, ch.ColType)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("ADD COLUMN %s.%s (%s)", ch.Table, ch.Column, ch.ColType))

	case ddlDropColumn:
		dh.schemaRegistry.RecordDropColumn(ch.Table, ch.Column)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: DROP COLUMN %s.%s", dh.connID, ch.Table, ch.Column)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("DROP COLUMN %s.%s", ch.Table, ch.Column))

	case ddlRenameColumn:
		dh.schemaRegistry.RecordRenameColumn(ch.Table, ch.OldName, ch.NewName)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: RENAME COLUMN %s.%s → %s", dh.connID, ch.Table, ch.OldName, ch.NewName)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("RENAME COLUMN %s.%s -> %s", ch.Table, ch.OldName, ch.NewName))

	case ddlAlterType:
		dh.schemaRegistry.RecordTypeChange(ch.Table, ch.Column, ch.OldType, ch.NewType)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: ALTER TYPE %s.%s → %s", dh.connID, ch.Table, ch.Column, ch.NewType)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("ALTER TYPE %s.%s -> %s", ch.Table, ch.Column, ch.NewType))

	case ddlDropTable:
		dh.schemaRegistry.RemoveTable(ch.Table)
		if dh.deltaMap != nil {
			dh.deltaMap.ClearTable(ch.Table)
		}
		if dh.tombstones != nil {
			dh.tombstones.ClearTable(ch.Table)
		}
		if dh.verbose {
			log.Printf("[conn %d] schema registry: DROP TABLE %s", dh.connID, ch.Table)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("DROP TABLE %s", ch.Table))

	case ddlCreateTable:
		dh.schemaRegistry.RecordNewTable(ch.Table)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: CREATE TABLE %s", dh.connID, ch.Table)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("CREATE TABLE %s", ch.Table))
	}
}

// forwardAndRelayDDL sends a message to the backend, relays the complete response
// to the client, and reports whether the backend returned an error.
func forwardAndRelayDDL(raw []byte, backend, client net.Conn) (hadError bool, err error) {
	if _, err := backend.Write(raw); err != nil {
		return false, fmt.Errorf("sending to backend: %w", err)
	}

	for {
		msg, err := readMsg(backend)
		if err != nil {
			return hadError, fmt.Errorf("reading backend response: %w", err)
		}

		if msg.Type == 'E' {
			hadError = true
		}

		if _, err := client.Write(msg.Raw); err != nil {
			return hadError, fmt.Errorf("relaying to client: %w", err)
		}

		if msg.Type == 'Z' {
			return hadError, nil
		}
	}
}

// stripFKConstraints parses a DDL statement and removes any FOREIGN KEY /
// REFERENCES constraints. Returns the modified SQL and a list of referenced
// table names. Returns ("", nil) if no FK constraints were found.
func stripFKConstraints(sql string) (string, []string) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return "", nil
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return "", nil
	}

	node := stmts[0].GetStmt()
	if node == nil {
		return "", nil
	}

	var refTables []string
	modified := false

	// ALTER TABLE ADD COLUMN with inline REFERENCES.
	if alt := node.GetAlterTableStmt(); alt != nil {
		for _, cmdNode := range alt.GetCmds() {
			cmd := cmdNode.GetAlterTableCmd()
			if cmd == nil {
				continue
			}

			switch cmd.GetSubtype() {
			case pg_query.AlterTableType_AT_AddColumn:
				// Strip FK constraints from column definition.
				if def := cmd.GetDef(); def != nil {
					if colDef := def.GetColumnDef(); colDef != nil {
						var kept []*pg_query.Node
						for _, c := range colDef.GetConstraints() {
							con := c.GetConstraint()
							if con != nil && con.GetContype() == pg_query.ConstrType_CONSTR_FOREIGN {
								// Found a FK constraint — strip it.
								if pktable := con.GetPktable(); pktable != nil {
									refTables = append(refTables, rangeVarName(pktable))
								}
								modified = true
								continue
							}
							kept = append(kept, c)
						}
						colDef.Constraints = kept
					}
				}

			case pg_query.AlterTableType_AT_AddConstraint:
				// ALTER TABLE ADD CONSTRAINT ... FOREIGN KEY (...)
				if def := cmd.GetDef(); def != nil {
					if con := def.GetConstraint(); con != nil && con.GetContype() == pg_query.ConstrType_CONSTR_FOREIGN {
						if pktable := con.GetPktable(); pktable != nil {
							refTables = append(refTables, rangeVarName(pktable))
						}
						modified = true
						// Replace the entire command with a no-op by clearing the cmd.
						// We can't remove it from the list easily, so we'll filter later.
					}
				}
			}
		}

		// If we found AT_AddConstraint FK commands, filter them out.
		if modified {
			var keptCmds []*pg_query.Node
			for _, cmdNode := range alt.GetCmds() {
				cmd := cmdNode.GetAlterTableCmd()
				if cmd != nil && cmd.GetSubtype() == pg_query.AlterTableType_AT_AddConstraint {
					if def := cmd.GetDef(); def != nil {
						if con := def.GetConstraint(); con != nil && con.GetContype() == pg_query.ConstrType_CONSTR_FOREIGN {
							continue // Skip FK constraint commands.
						}
					}
				}
				keptCmds = append(keptCmds, cmdNode)
			}
			// Only update if we actually removed commands and have commands remaining.
			if len(keptCmds) < len(alt.GetCmds()) {
				if len(keptCmds) == 0 {
					// All commands were FK constraints — return a no-op.
					return "SELECT 1", refTables
				}
				alt.Cmds = keptCmds
			}
		}
	}

	// CREATE TABLE: strip FK constraints from column defs and table constraints.
	if create := node.GetCreateStmt(); create != nil {
		var keptElts []*pg_query.Node
		for _, elt := range create.GetTableElts() {
			// Column definition with inline REFERENCES.
			if colDef := elt.GetColumnDef(); colDef != nil {
				var kept []*pg_query.Node
				for _, c := range colDef.GetConstraints() {
					con := c.GetConstraint()
					if con != nil && con.GetContype() == pg_query.ConstrType_CONSTR_FOREIGN {
						if pktable := con.GetPktable(); pktable != nil {
							refTables = append(refTables, rangeVarName(pktable))
						}
						modified = true
						continue
					}
					kept = append(kept, c)
				}
				colDef.Constraints = kept
				keptElts = append(keptElts, elt)
				continue
			}

			// Table-level FOREIGN KEY constraint.
			if con := elt.GetConstraint(); con != nil && con.GetContype() == pg_query.ConstrType_CONSTR_FOREIGN {
				if pktable := con.GetPktable(); pktable != nil {
					refTables = append(refTables, rangeVarName(pktable))
				}
				modified = true
				continue
			}

			keptElts = append(keptElts, elt)
		}
		if modified {
			create.TableElts = keptElts
		}
	}

	if !modified {
		return "", nil
	}

	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return "", nil
	}

	return deparsed, refTables
}
