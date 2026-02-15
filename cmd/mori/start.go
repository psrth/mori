package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/logging"
	morimcp "github.com/mori-dev/mori/internal/mcp"
	"github.com/mori-dev/mori/internal/registry"
	"github.com/spf13/cobra"

	// Register engine implementations via side-effect imports.
	_ "github.com/mori-dev/mori/internal/engine/firestore"
	_ "github.com/mori-dev/mori/internal/engine/mssql"
	_ "github.com/mori-dev/mori/internal/engine/mysql"
	_ "github.com/mori-dev/mori/internal/engine/oracle"
	_ "github.com/mori-dev/mori/internal/engine/postgres"
	_ "github.com/mori-dev/mori/internal/engine/redis"
	_ "github.com/mori-dev/mori/internal/engine/sqlite"
)

var startCmd = &cobra.Command{
	Use:   "start [connection-name]",
	Short: "Start the Mori proxy",
	Long: `Start the Mori proxy for a named connection from mori.yaml.

If no connection name is given and only one connection exists, it is selected
automatically. If multiple connections exist, an interactive picker is shown.

On first start for a connection, the Shadow database container is created
and the production schema is dumped. Subsequent starts reuse the existing
Shadow.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runStart,
}

func init() {
	startCmd.Flags().IntP("port", "p", 9002, "Port for the proxy to listen on")
	startCmd.Flags().Bool("verbose", false, "Log all intercepted queries and routing decisions")
	startCmd.Flags().Bool("mcp", false, "Enable MCP server for AI tool integration")
	startCmd.Flags().Int("mcp-port", 9000, "Port for the MCP server")
}

func runStart(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	verbose, _ := cmd.Flags().GetBool("verbose")
	mcpEnabled, _ := cmd.Flags().GetBool("mcp")
	mcpPort, _ := cmd.Flags().GetInt("mcp-port")

	// 1. Find project root.
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	// 2. Read mori.yaml.
	if !config.HasProjectConfig(projectRoot) {
		return fmt.Errorf("no mori.yaml found — run 'mori init' to configure a connection")
	}
	projCfg, err := config.ReadProjectConfig(projectRoot)
	if err != nil {
		return fmt.Errorf("failed to read mori.yaml: %w", err)
	}
	if len(projCfg.Connections) == 0 {
		return fmt.Errorf("no connections in mori.yaml — run 'mori init' to add one")
	}

	// 3. Determine which connection to start.
	connName, err := resolveConnectionName(projCfg, args)
	if err != nil {
		return err
	}
	conn := projCfg.GetConnection(connName)
	if conn == nil {
		return fmt.Errorf("connection %q not found in mori.yaml", connName)
	}

	// 4. Look up the engine implementation.
	eng, err := engine.Lookup(registry.EngineID(conn.Engine))
	if err != nil {
		return fmt.Errorf("engine %q is not yet supported — only PostgreSQL and CockroachDB connections can be started", conn.Engine)
	}

	// 5. Check for existing active connection.
	var cfg *config.Config
	if config.IsInitialized(projectRoot) {
		cfg, err = config.ReadConfig(projectRoot)
		if err != nil {
			return fmt.Errorf("failed to read runtime config: %w", err)
		}
		if cfg.ActiveConnection != "" && cfg.ActiveConnection != connName {
			return fmt.Errorf("connection %q is currently active — run 'mori stop' first, then 'mori start %s'",
				cfg.ActiveConnection, connName)
		}
	}

	// 6. If no runtime config exists, run the engine init (first start).
	if cfg == nil {
		fmt.Printf("First start for %q — setting up Shadow database...\n\n", connName)
		authProvider := auth.Lookup(registry.ProviderID(conn.Provider))
		connStr, err := authProvider.ConnString(cmd.Context(), conn)
		if err != nil {
			return fmt.Errorf("failed to build connection string: %w", err)
		}
		result, err := eng.Init(cmd.Context(), engine.InitOptions{
			ProdConnStr: connStr,
			ProjectRoot: projectRoot,
		})
		if err != nil {
			return err
		}
		cfg = result.Config
		cfg.ActiveConnection = connName
		if err := config.WriteConfig(projectRoot, cfg); err != nil {
			return fmt.Errorf("failed to update config with active connection: %w", err)
		}

		fmt.Println()
		fmt.Printf("Shadow ready — %s %s\n", cfg.Engine, cfg.EngineVersion)
		fmt.Printf("  Shadow: localhost:%d (container: %s)\n", result.ContainerPort, result.ContainerName)
		fmt.Printf("  Tables: %d\n", result.TableCount)
		if len(cfg.Extensions) > 0 {
			fmt.Printf("  Extensions: %s\n", strings.Join(cfg.Extensions, ", "))
		}
		fmt.Println()
	}

	// 7. Parse prod address from config.
	connInfo, err := eng.ParseConnStr(cfg.ProdConnection)
	if err != nil {
		return fmt.Errorf("invalid prod connection in config: %w", err)
	}
	prodAddr := connInfo.Addr

	// 8. Check for an existing proxy (stale PID file).
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

	// 9. Write PID file.
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	// 10. Load table metadata for classifier.
	moriDir := config.MoriDirPath(projectRoot)
	tables, err := eng.LoadTableMeta(moriDir)
	if err != nil {
		if verbose {
			log.Printf("Warning: could not load table metadata: %v (classifier will run without PK info)", err)
		}
		tables = make(map[string]engine.TableMeta)
	}

	// 11. Create classifier.
	classifier := eng.NewClassifier(tables)

	// 12. Load delta state.
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

	// 13. Load schema registry.
	schemaReg := coreSchema.NewRegistry()
	if sr, err := coreSchema.ReadRegistry(moriDir); err == nil {
		schemaReg = sr
	} else if verbose {
		log.Printf("No existing schema registry found (starting clean): %v", err)
	}

	// 14. Create router.
	router := core.NewRouter(deltaMap, tombstones, schemaReg)

	// 15. Compute Shadow address.
	shadowAddr := fmt.Sprintf("127.0.0.1:%d", cfg.ShadowPort)

	// 16. Create structured logger.
	var logger *logging.Logger
	if l, err := logging.New(config.LogFilePath(projectRoot)); err != nil {
		log.Printf("Warning: could not create log file: %v (structured logging disabled)", err)
	} else {
		logger = l
	}

	// 17. Create proxy.
	p := eng.NewProxy(engine.ProxyDeps{
		ProdAddr:   prodAddr,
		ShadowAddr: shadowAddr,
		DBName:     connInfo.DBName,
		ListenPort: port,
		Verbose:    verbose,
		Classifier: classifier,
		Router:     router,
		DeltaMap:   deltaMap,
		Tombstones: tombstones,
		SchemaReg:  schemaReg,
		MoriDir:    moriDir,
		Logger:     logger,
	}, tables)

	// 18. Set up signal handling.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.ListenAndServe(ctx)
	}()

	// 19. Optionally start MCP server.
	var mcpSrv *morimcp.Server
	if mcpEnabled {
		mcpSrv = morimcp.New(mcpPort, port, connInfo.DBName, connInfo.User, connInfo.Password)
		go func() {
			if err := mcpSrv.ListenAndServe(ctx); err != nil {
				log.Printf("MCP server error: %v", err)
			}
		}()
	}

	fmt.Printf("Mori proxy started on 127.0.0.1:%d → %s [%s]\n", port, prodAddr, connName)
	fmt.Printf("  Prod:   %s\n", cfg.RedactedProdConnection())
	fmt.Printf("  Shadow: %s\n", shadowAddr)
	if mcpEnabled {
		fmt.Printf("  MCP:    http://127.0.0.1:%d/mcp\n", mcpPort)
	}
	if verbose {
		fmt.Println("  Verbose logging enabled.")
	}
	fmt.Println("Press Ctrl+C to stop.")

	// 20. Wait for signal or error.
	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived %s, shutting down...\n", sig)
	case err := <-errCh:
		if err != nil {
			fmt.Printf("Proxy error: %v\n", err)
		}
	}

	// 21. Graceful shutdown with 10-second timeout.
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if mcpSrv != nil {
		if err := mcpSrv.Shutdown(shutdownCtx); err != nil {
			log.Printf("MCP server shutdown error: %v", err)
		}
	}
	if err := p.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown timeout: %v", err)
	}

	// 22. Remove PID file.
	os.Remove(pidPath)
	fmt.Printf("Mori proxy stopped [%s].\n", connName)

	return nil
}

// resolveConnectionName determines which connection to use based on args and config.
func resolveConnectionName(projCfg *config.ProjectConfig, args []string) (string, error) {
	// Explicit name provided.
	if len(args) == 1 {
		return args[0], nil
	}

	names := projCfg.ConnectionNames()

	// Only one connection — auto-select.
	if len(names) == 1 {
		return names[0], nil
	}

	// Multiple connections — interactive picker.
	var options []huh.Option[string]
	for _, name := range names {
		conn := projCfg.GetConnection(name)
		label := fmt.Sprintf("%s  (%s / %s)", name, conn.Engine, conn.Provider)
		if conn.Host != "" {
			label = fmt.Sprintf("%s  (%s @ %s)", name, conn.Engine, conn.Host)
		}
		options = append(options, huh.NewOption(label, name))
	}

	var selected string
	err := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Select connection to start").
				Options(options...).
				Value(&selected),
		),
	).Run()
	if err != nil {
		return "", err
	}

	return selected, nil
}
