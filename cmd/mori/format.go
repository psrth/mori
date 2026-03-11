package main

import (
	"strings"

	coreSchema "github.com/psrth/mori/internal/core/schema"
	"github.com/psrth/mori/internal/ui"
)

func pluralize(n int, singular, plural string) string {
	return ui.Pluralize(n, singular, plural)
}

func formatNumber(n int64) string {
	return ui.FormatNumber(n)
}

func formatSchemaDiff(diff *coreSchema.TableDiff) string {
	return ui.FormatSchemaDiff(diff)
}

func isProxyRunning(pidPath string) (int, bool) {
	return ui.IsProxyRunning(pidPath)
}

// quoteIdent quotes a PostgreSQL identifier.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
