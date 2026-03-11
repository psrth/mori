package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/engine/redis/connstr"
	"github.com/psrth/mori/internal/ui"
	"github.com/psrth/mori/internal/engine/redis/schema"
	"github.com/psrth/mori/internal/engine/redis/shadow"
)

// InitOptions holds the options for initializing a Redis Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
	ConnName    string
}

// InitResult holds the result of a successful initialization.
type InitResult struct {
	Config *config.Config
	Tables map[string]schema.KeyMeta
}

// Init performs the complete Mori initialization sequence for Redis.
func Init(ctx context.Context, opts InitOptions) (*InitResult, error) {
	// 1. Parse the connection string.
	info, err := connstr.Parse(opts.ProdConnStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// 2. Ping Redis.
	var infoOutput string
	if err := ui.Spinner("Connecting to production Redis...", func() error {
		if e := schema.PingRedis(info); e != nil {
			return e
		}
		var e error
		infoOutput, e = schema.GetRedisInfo(info)
		return e
	}); err != nil {
		return nil, fmt.Errorf("cannot connect to production Redis: %w", err)
	}

	// 3. Get Redis version.
	version := schema.ParseRedisVersion(infoOutput)
	ui.StepDone(fmt.Sprintf("Connected — Redis %s", version))

	// 4. Create .mori directory.
	moriDir := config.ConnDir(opts.ProjectRoot, opts.ConnName)
	if err := config.InitConnDir(opts.ProjectRoot, opts.ConnName); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	// 5. Start shadow Redis container, matching production version.
	mgr, err := shadow.NewManager()
	if err != nil {
		return nil, fmt.Errorf("Docker not available: %w", err)
	}
	defer mgr.Close()

	shadowImage := shadow.ImageForVersion(version)
	if err := ui.Spinner(fmt.Sprintf("Pulling Redis image %s...", shadowImage), func() error {
		if e := mgr.Pull(ctx, shadowImage); e != nil {
			// Fall back to default image if version-matched image is unavailable.
			shadowImage = shadow.DefaultImage
			return mgr.Pull(ctx, shadowImage)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to pull Redis image: %w", err)
	}

	var containerInfo *shadow.ContainerInfo
	if err := ui.Spinner("Creating Shadow Redis container...", func() error {
		var e error
		containerInfo, e = mgr.Create(ctx, shadow.ContainerConfig{
			Image:    shadowImage,
			HostPort: shadow.DefaultHostPort,
		})
		return e
	}); err != nil {
		return nil, fmt.Errorf("failed to create shadow container: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Shadow container ready on port %d", containerInfo.HostPort))

	// 6. Discover key patterns.
	tables, err := schema.DetectKeyMetadata(info)
	if err != nil {
		ui.StepWarn(fmt.Sprintf("Key detection failed: %v (starting with empty metadata)", err))
		tables = make(map[string]schema.KeyMeta)
	} else {
		ui.StepDone(fmt.Sprintf("%d key prefixes detected", len(tables)))
	}

	// 7. Persist configuration.
	cfg := &config.Config{
		ProdConnection:  info.Raw,
		ShadowPort:      containerInfo.HostPort,
		ShadowContainer: containerInfo.ContainerName,
		ShadowImage:     containerInfo.Image,
		Engine:          "redis",
		EngineVersion:   version,
		ProxyPort:       9002,
		InitializedAt:   time.Now(),
	}

	if err := config.WriteConnConfig(opts.ProjectRoot, opts.ConnName, cfg); err != nil {
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	if err := schema.WriteTables(moriDir, tables); err != nil {
		return nil, fmt.Errorf("failed to write tables: %w", err)
	}

	return &InitResult{
		Config: cfg,
		Tables: tables,
	}, nil
}
