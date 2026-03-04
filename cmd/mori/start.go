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
	"github.com/mori-dev/mori/internal/tunnel"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"

	// Register engine implementations via side-effect imports.
	_ "github.com/mori-dev/mori/internal/tunnel/tunnels"
	_ "github.com/mori-dev/mori/internal/engine/duckdb"
	_ "github.com/mori-dev/mori/internal/engine/firestore"
	_ "github.com/mori-dev/mori/internal/engine/mssql"
	_ "github.com/mori-dev/mori/internal/engine/mysql"
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

Multiple connections can run simultaneously on different ports.

On first start for a connection, the Shadow database container is created
and the production schema is dumped. Subsequent starts reuse the existing
Shadow.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runStart,
}

func init() {
	startCmd.Flags().IntP("port", "p", 0, "Port for the proxy to listen on (default: auto-assign from 9002)")
	startCmd.Flags().Bool("verbose", false, "Log all intercepted queries and routing decisions")
	startCmd.Flags().Bool("mcp", false, "Enable MCP server for AI tool integration")
	startCmd.Flags().Int("mcp-port", 9000, "Port for the MCP server")
}

func runStart(cmd *cobra.Command, args []string) error {
	portFlag, _ := cmd.Flags().GetInt("port")
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

	ui.StepDone(fmt.Sprintf("Loaded connection %s", ui.Cyan(connName)))

	// 4. Look up the engine implementation.
	eng, err := engine.Lookup(registry.EngineID(conn.Engine))
	if err != nil {
		return fmt.Errorf("engine %q is not yet supported — only PostgreSQL and CockroachDB connections can be started", conn.Engine)
	}

	// 5. Migrate legacy state if needed (old .mori/config.json → .mori/<connName>/).
	config.MigrateToConnDir(projectRoot, connName)

	// 6. Check if this connection is already running.
	connDir := config.ConnDir(projectRoot, connName)
	pidPath := config.ConnPidFilePath(projectRoot, connName)
	if pid, running := isProxyRunning(pidPath); running {
		return fmt.Errorf("connection %q is already running (PID %d) — run 'mori stop %s' first", connName, pid, connName)
	}

	// 7. Start tunnel if configured.
	var tunnelMgr *tunnel.Manager
	if conn.Tunnel != nil {
		tunnelMgr, err = tunnel.NewManager(conn.Tunnel)
		if err != nil {
			return fmt.Errorf("tunnel setup failed: %w", err)
		}
	}
	if tunnelMgr != nil {
		tunnelErr := ui.Spinner(fmt.Sprintf("Starting %s...", tunnelMgr.DisplayName()), func() error {
			return tunnelMgr.Start(cmd.Context())
		})
		if tunnelErr != nil {
			return fmt.Errorf("failed to start tunnel: %w", tunnelErr)
		}
		ui.StepDone(fmt.Sprintf("Tunnel ready on %s", tunnelMgr.LocalAddr()))

		// Ensure the per-connection state directory exists before writing the PID file.
		if err := config.InitConnDir(projectRoot, connName); err != nil {
			log.Printf("Warning: could not create connection state directory: %v", err)
		}

		// Write tunnel PID file for stop command.
		tunnelPidPath := config.ConnTunnelPidFilePath(projectRoot, connName)
		if err := os.WriteFile(tunnelPidPath, []byte(strconv.Itoa(tunnelMgr.PID())), 0644); err != nil {
			log.Printf("Warning: could not write tunnel PID file: %v", err)
		}

		// Rewrite connection to go through the tunnel.
		// The tunnel handles TLS to the remote database, so the local
		// connection must use plain TCP (sslmode=disable).
		conn.Host = "127.0.0.1"
		conn.Port = tunnelMgr.LocalPort()
		conn.SSLMode = "disable"
	}

	// 8. If no runtime config exists for this connection, run the engine init (first start).
	var cfg *config.Config
	if config.IsConnInitialized(projectRoot, connName) {
		cfg, err = config.ReadConnConfig(projectRoot, connName)
		if err != nil {
			return fmt.Errorf("failed to read runtime config: %w", err)
		}
	} else {
		authProvider := auth.Lookup(registry.ProviderID(conn.Provider))
		connStr, err := authProvider.ConnString(cmd.Context(), conn)
		if err != nil {
			return fmt.Errorf("failed to build connection string: %w", err)
		}
		imageOverride := ""
		if conn.Extra != nil {
			imageOverride = conn.Extra["image"]
		}

		result, initErr := eng.Init(cmd.Context(), engine.InitOptions{
			ProdConnStr:   connStr,
			ProjectRoot:   projectRoot,
			ConnName:      connName,
			ImageOverride: imageOverride,
		})
		if initErr != nil {
			return initErr
		}
		cfg = result.Config

		tableLabel := fmt.Sprintf("%d %s", result.TableCount, pluralize(result.TableCount, "table", "tables"))
		ui.StepDone(fmt.Sprintf("Shadow ready — %s %s · %s", cfg.Engine, cfg.EngineVersion, tableLabel))
	}

	// 8b. If a tunnel is active, refresh the prod connection string in the config
	// with the current tunnel address (the ephemeral port changes each restart).
	if tunnelMgr != nil && cfg != nil {
		authProvider := auth.Lookup(registry.ProviderID(conn.Provider))
		freshConnStr, err := authProvider.ConnString(cmd.Context(), conn)
		if err != nil {
			return fmt.Errorf("failed to rebuild connection string for tunnel: %w", err)
		}
		cfg.ProdConnection = freshConnStr
	}

	// 9. Determine proxy port: explicit flag > existing config > auto-assign.
	port := portFlag
	if port == 0 && cfg.ProxyPort > 0 {
		// Reuse previously assigned port, but verify it's not taken by another connection.
		otherUsing := false
		for _, name := range config.InitializedConnections(projectRoot) {
			if name == connName {
				continue
			}
			otherPid := config.ConnPidFilePath(projectRoot, name)
			if _, running := isProxyRunning(otherPid); running {
				if otherCfg, err := config.ReadConnConfig(projectRoot, name); err == nil {
					if otherCfg.ProxyPort == cfg.ProxyPort {
						otherUsing = true
						break
					}
				}
			}
		}
		if !otherUsing {
			port = cfg.ProxyPort
		}
	}
	if port == 0 {
		port = config.NextAvailablePort(projectRoot, 9002)
	}

	// Update port in config.
	cfg.ProxyPort = port
	cfg.ActiveConnection = connName
	if err := config.WriteConnConfig(projectRoot, connName, cfg); err != nil {
		return fmt.Errorf("failed to update config: %w", err)
	}

	// 10. Parse prod address from config.
	connInfo, err := eng.ParseConnStr(cfg.ProdConnection)
	if err != nil {
		return fmt.Errorf("invalid prod connection in config: %w", err)
	}
	prodAddr := connInfo.Addr

	// 11. Check for a stale PID file.
	if data, err := os.ReadFile(pidPath); err == nil {
		if pid, err := strconv.Atoi(string(data)); err == nil {
			if process, err := os.FindProcess(pid); err == nil {
				if err := process.Signal(syscall.Signal(0)); err == nil {
					return fmt.Errorf("mori proxy already running (PID %d) — run 'mori stop %s' first", pid, connName)
				}
			}
		}
		os.Remove(pidPath)
	}

	// 12. Write PID file.
	if err := config.InitConnDir(projectRoot, connName); err != nil {
		return fmt.Errorf("failed to create state directory: %w", err)
	}
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	// 13. Load table metadata for classifier.
	tables, err := eng.LoadTableMeta(connDir)
	if err != nil {
		if verbose {
			log.Printf("Warning: could not load table metadata: %v (classifier will run without PK info)", err)
		}
		tables = make(map[string]engine.TableMeta)
	}

	// 14. Create classifier.
	classifier := eng.NewClassifier(tables)

	// 15. Load delta state.
	deltaMap := delta.NewMap()
	if dm, err := delta.ReadDeltaMap(connDir); err == nil {
		deltaMap = dm
	} else if verbose {
		log.Printf("No existing delta map found (starting clean): %v", err)
	}

	tombstones := delta.NewTombstoneSet()
	if ts, err := delta.ReadTombstoneSet(connDir); err == nil {
		tombstones = ts
	} else if verbose {
		log.Printf("No existing tombstones found (starting clean): %v", err)
	}

	// 16. Load schema registry.
	schemaReg := coreSchema.NewRegistry()
	if sr, err := coreSchema.ReadRegistry(connDir); err == nil {
		schemaReg = sr
	} else if verbose {
		log.Printf("No existing schema registry found (starting clean): %v", err)
	}

	// 17. Create router.
	router := core.NewRouter(deltaMap, tombstones, schemaReg)

	// 18. Compute Shadow address.
	shadowAddr := fmt.Sprintf("127.0.0.1:%d", cfg.ShadowPort)

	// 19. Create structured logger.
	var logger *logging.Logger
	logPath := config.ConnLogFilePath(projectRoot, connName)
	if l, err := logging.New(logPath); err != nil {
		log.Printf("Warning: could not create log file: %v (structured logging disabled)", err)
	} else {
		logger = l
	}

	// 20. Create proxy.
	p := eng.NewProxy(engine.ProxyDeps{
		ProdAddr:       prodAddr,
		ShadowAddr:     shadowAddr,
		DBName:         connInfo.DBName,
		ListenPort:     port,
		Verbose:        verbose,
		Classifier:     classifier,
		Router:         router,
		DeltaMap:       deltaMap,
		Tombstones:     tombstones,
		SchemaReg:      schemaReg,
		MoriDir:        connDir,
		Logger:         logger,
		MaxRowsHydrate: cfg.MaxRowsHydrate,
	}, tables)

	// 21. Set up signal handling.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.ListenAndServe(ctx)
	}()

	// 22. Optionally start MCP server.
	var mcpSrv *morimcp.Server
	if mcpEnabled {
		mcpSrv = morimcp.New(mcpPort, morimcp.EngineConfig{
			Engine:    cfg.Engine,
			ProxyPort: port,
			DBName:    connInfo.DBName,
			User:      connInfo.User,
			Password:  connInfo.Password,
		})
		go func() {
			if err := mcpSrv.ListenAndServe(ctx); err != nil {
				log.Printf("MCP server error: %v", err)
			}
		}()
	}

	// Monitor tunnel health if active.
	if tunnelMgr != nil {
		go func() {
			select {
			case err := <-tunnelMgr.Done():
				if err != nil && ctx.Err() == nil {
					fmt.Println()
					ui.StepWarn(fmt.Sprintf("Tunnel died unexpectedly: %v", err))
					ui.Info("Database connections may fail. Restart with 'mori stop && mori start'.")
				}
			case <-ctx.Done():
			}
		}()
	}

	ui.StepDone(fmt.Sprintf("Proxy listening on 127.0.0.1:%d %s %s", port, ui.IconArrow, prodAddr))
	ui.Info(fmt.Sprintf("Prod:   %s", cfg.RedactedProdConnection()))
	ui.Info(fmt.Sprintf("Shadow: %s", shadowAddr))
	if mcpEnabled {
		ui.Info(fmt.Sprintf("MCP:    http://127.0.0.1:%d/mcp", mcpPort))
	}
	if verbose {
		ui.Info("Verbose logging enabled.")
	}
	ui.Info("Press Ctrl+C to stop.")

	// 23. Wait for signal or error.
	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived %s, shutting down...\n", sig)
	case err := <-errCh:
		if err != nil {
			fmt.Printf("Proxy error: %v\n", err)
		}
	}

	// 24. Graceful shutdown with 10-second timeout.
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

	// 25. Stop tunnel if active.
	if tunnelMgr != nil {
		if err := tunnelMgr.Stop(); err != nil {
			log.Printf("Warning: failed to stop tunnel: %v", err)
		}
		os.Remove(config.ConnTunnelPidFilePath(projectRoot, connName))
	}

	// 26. Remove PID file.
	os.Remove(pidPath)
	ui.StepDone(fmt.Sprintf("Proxy stopped %s.", ui.Cyan(connName)))

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
