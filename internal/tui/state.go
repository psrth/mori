package tui

import (
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
	pgschema "github.com/psrth/mori/internal/engine/postgres/schema"
	"github.com/psrth/mori/internal/ui"
)

// Snapshot holds all mori state read from disk in a single pass.
// Fields are nil/zero when the corresponding file could not be read.
type Snapshot struct {
	Config       *config.Config
	ProjConfig   *config.ProjectConfig
	DeltaMap     *delta.Map
	Tombstones   *delta.TombstoneSet
	SchemaReg    *coreSchema.Registry
	Sequences    map[string]pgschema.SequenceOffset
	Tables       map[string]pgschema.TableMeta
	ProxyPID     int
	ProxyRunning bool
}

// ReadSnapshot reads all state files from disk for a specific connection.
// Never returns an error — returns whatever it can read successfully.
func ReadSnapshot(projectRoot, connName, moriDir string) Snapshot {
	var snap Snapshot

	snap.Config, _ = config.ReadConnConfig(projectRoot, connName)

	if config.HasProjectConfig(projectRoot) {
		snap.ProjConfig, _ = config.ReadProjectConfig(projectRoot)
	}

	snap.DeltaMap, _ = delta.ReadDeltaMap(moriDir)
	snap.Tombstones, _ = delta.ReadTombstoneSet(moriDir)
	snap.SchemaReg, _ = coreSchema.ReadRegistry(moriDir)
	snap.Sequences, _ = pgschema.ReadSequences(moriDir)
	snap.Tables, _ = pgschema.ReadTables(moriDir)

	pidPath := config.ConnPidFilePath(projectRoot, connName)
	snap.ProxyPID, snap.ProxyRunning = ui.IsProxyRunning(pidPath)

	return snap
}
