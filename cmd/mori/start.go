package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/classify"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
	"github.com/mori-dev/mori/internal/engine/postgres/proxy"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the Mori proxy",
	Long: `Start the Mori proxy, which listens on localhost and accepts database
connections from your application. All traffic is forwarded to the production
database transparently.`,
	RunE: runStart,
}

func init() {
	startCmd.Flags().IntP("port", "p", 5432, "Port for the proxy to listen on")
	startCmd.Flags().Bool("verbose", false, "Log all intercepted queries and routing decisions")
}

func runStart(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	verbose, _ := cmd.Flags().GetBool("verbose")

	// 1. Find project root and read config.
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}
	if !config.IsInitialized(projectRoot) {
		return fmt.Errorf("mori is not initialized — run 'mori init --from <conn_string>' first")
	}

	cfg, err := config.ReadConfig(projectRoot)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	// 2. Parse prod address from config.
	dsn, err := connstr.Parse(cfg.ProdConnection)
	if err != nil {
		return fmt.Errorf("invalid prod connection in config: %w", err)
	}
	prodAddr := dsn.Address()

	// 3. Check for an existing proxy (stale PID file).
	pidPath := config.PidFilePath(projectRoot)
	if data, err := os.ReadFile(pidPath); err == nil {
		if pid, err := strconv.Atoi(string(data)); err == nil {
			if process, err := os.FindProcess(pid); err == nil {
				if err := process.Signal(syscall.Signal(0)); err == nil {
					return fmt.Errorf("mori proxy already running (PID %d) — run 'mori stop' first", pid)
				}
			}
		}
		os.Remove(pidPath)
	}

	// 4. Write PID file.
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	// 5. Load table metadata for classifier.
	moriDir := config.MoriDirPath(projectRoot)
	tables, err := schema.ReadTables(moriDir)
	if err != nil {
		if verbose {
			log.Printf("Warning: could not load table metadata: %v (classifier will run without PK info)", err)
		}
		tables = make(map[string]schema.TableMeta)
	}

	// 6. Create classifier.
	classifier := classify.New(tables)

	// 7. Load delta state.
	deltaMap := delta.NewMap()
	if dm, err := delta.ReadDeltaMap(moriDir); err == nil {
		deltaMap = dm
	} else if verbose {
		log.Printf("No existing delta map found (starting clean): %v", err)
	}

	tombstones := delta.NewTombstoneSet()
	if ts, err := delta.ReadTombstoneSet(moriDir); err == nil {
		tombstones = ts
	} else if verbose {
		log.Printf("No existing tombstones found (starting clean): %v", err)
	}

	// 8. Load schema registry.
	var schemaReg *coreSchema.Registry
	if sr, err := coreSchema.ReadRegistry(moriDir); err == nil {
		schemaReg = sr
	} else if verbose {
		log.Printf("No existing schema registry found (starting clean): %v", err)
	}

	// 9. Create router.
	router := core.NewRouter(deltaMap, tombstones)

	// 10. Compute Shadow address.
	shadowAddr := fmt.Sprintf("127.0.0.1:%d", cfg.ShadowPort)

	// 11. Create proxy.
	p := proxy.New(prodAddr, shadowAddr, dsn.DBName, port, verbose, classifier, router,
		deltaMap, tombstones, tables, moriDir, schemaReg)

	// 11. Set up signal handling.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.ListenAndServe(ctx)
	}()

	fmt.Printf("Mori proxy started on 127.0.0.1:%d → %s\n", port, prodAddr)
	fmt.Printf("  Prod:   %s\n", cfg.RedactedProdConnection())
	fmt.Printf("  Shadow: %s\n", shadowAddr)
	if verbose {
		fmt.Println("  Verbose logging enabled.")
	}
	fmt.Println("Press Ctrl+C to stop.")

	// 12. Wait for signal or error.
	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived %s, shutting down...\n", sig)
	case err := <-errCh:
		if err != nil {
			fmt.Printf("Proxy error: %v\n", err)
		}
	}

	// 13. Graceful shutdown with 10-second timeout.
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := p.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown timeout: %v", err)
	}

	// 14. Remove PID file.
	os.Remove(pidPath)
	fmt.Println("Mori proxy stopped.")

	return nil
}
