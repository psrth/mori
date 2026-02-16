package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/redis/connstr"
	"github.com/mori-dev/mori/internal/engine/redis/schema"
	"github.com/mori-dev/mori/internal/engine/redis/shadow"
)

// InitOptions holds the options for initializing a Redis Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
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
	fmt.Println("Connecting to production Redis...")
	if err := schema.PingRedis(info); err != nil {
		return nil, fmt.Errorf("cannot connect to production Redis: %w", err)
	}

	// 3. Get Redis version.
	fmt.Println("Detecting Redis version...")
	infoOutput, err := schema.GetRedisInfo(info)
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis info: %w", err)
	}
	version := schema.ParseRedisVersion(infoOutput)
	fmt.Printf("  Redis %s detected\n", version)

	// 4. Create .mori directory.
	moriDir := config.MoriDirPath(opts.ProjectRoot)
	if err := config.InitDir(opts.ProjectRoot); err != nil {
		return nil, fmt.Errorf("failed to create .mori directory: %w", err)
	}

	// 5. Start shadow Redis container, matching production version.
	fmt.Println("Creating Shadow Redis container...")
	mgr, err := shadow.NewManager()
	if err != nil {
		return nil, fmt.Errorf("Docker not available: %w", err)
	}
	defer mgr.Close()

	shadowImage := shadow.ImageForVersion(version)
	fmt.Printf("  Pulling %s (matching prod %s)...\n", shadowImage, version)
	if err := mgr.Pull(ctx, shadowImage); err != nil {
		// Fall back to default image if version-matched image is unavailable.
		fmt.Printf("  Warning: %s not available, falling back to %s\n", shadowImage, shadow.DefaultImage)
		shadowImage = shadow.DefaultImage
		if err := mgr.Pull(ctx, shadowImage); err != nil {
			return nil, fmt.Errorf("failed to pull Redis image: %w", err)
		}
	}

	containerInfo, err := mgr.Create(ctx, shadow.ContainerConfig{
		Image:    shadowImage,
		HostPort: shadow.DefaultHostPort,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create shadow container: %w", err)
	}
	fmt.Printf("  Shadow: localhost:%d (container: %s)\n", containerInfo.HostPort, containerInfo.ContainerName)

	// 6. Discover key patterns.
	fmt.Println("Detecting key patterns...")
	tables, err := schema.DetectKeyMetadata(info)
	if err != nil {
		// Non-fatal: Redis might be empty.
		fmt.Printf("  Warning: key detection failed: %v (starting with empty metadata)\n", err)
		tables = make(map[string]schema.KeyMeta)
	}
	fmt.Printf("  %d key prefixes\n", len(tables))

	// 7. Persist configuration.
	fmt.Println("Persisting configuration...")
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

	if err := config.WriteConfig(opts.ProjectRoot, cfg); err != nil {
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
