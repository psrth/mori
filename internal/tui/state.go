package tui

import (
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	pgschema "github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/ui"
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

// ReadSnapshot reads all state files from disk.
// Never returns an error — returns whatever it can read successfully.
func ReadSnapshot(projectRoot, moriDir string) Snapshot {
	var snap Snapshot

	snap.Config, _ = config.ReadConfig(projectRoot)

	if config.HasProjectConfig(projectRoot) {
		snap.ProjConfig, _ = config.ReadProjectConfig(projectRoot)
	}

	snap.DeltaMap, _ = delta.ReadDeltaMap(moriDir)
	snap.Tombstones, _ = delta.ReadTombstoneSet(moriDir)
	snap.SchemaReg, _ = coreSchema.ReadRegistry(moriDir)
	snap.Sequences, _ = pgschema.ReadSequences(moriDir)
	snap.Tables, _ = pgschema.ReadTables(moriDir)

	pidPath := config.PidFilePath(projectRoot)
	snap.ProxyPID, snap.ProxyRunning = ui.IsProxyRunning(pidPath)

	return snap
}
