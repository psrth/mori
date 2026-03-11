package merge

import (
	"context"

	"github.com/psrth/mori/internal/core"
)

// MergeEngine combines results from Prod and Shadow.
// Each engine implements this with its own row serialization and comparison.
type MergeEngine interface {
	// MergeSingleTable performs a merged read for a single-table SELECT.
	MergeSingleTable(ctx context.Context, query string, class *core.Classification) (*core.ResultSet, error)

	// MergeJoin performs the execute-on-Prod-patch-from-Shadow strategy for JOINs.
	MergeJoin(ctx context.Context, query string, class *core.Classification) (*core.ResultSet, error)
}
