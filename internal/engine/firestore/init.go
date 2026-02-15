package firestore

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/option"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/firestore/connstr"
	"github.com/mori-dev/mori/internal/engine/firestore/schema"
	"github.com/mori-dev/mori/internal/engine/firestore/shadow"
)

// InitOptions holds the options for initializing a Firestore Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
}

// InitResult holds the result of a successful initialization.
type InitResult struct {
	Config        *config.Config
	ContainerName string
	ContainerPort int
	Collections   map[string]schema.CollectionMeta
}

// Init performs the complete Mori initialization sequence for Firestore.
func Init(ctx context.Context, opts InitOptions) (*InitResult, error) {
	// 1. Parse the connection string.
	info, err := connstr.Parse(opts.ProdConnStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// 2. Connect to prod Firestore.
	fmt.Println("Connecting to production Firestore...")
	prodClient, err := newFirestoreClient(ctx, info)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to Firestore project %q: %w", info.ProjectID, err)
	}
	defer prodClient.Close()

	// 3. Detect collections.
	fmt.Println("Detecting collections...")
	collections, err := schema.DetectCollections(ctx, prodClient)
	if err != nil {
		return nil, fmt.Errorf("collection detection failed: %w", err)
	}
	fmt.Printf("  %d collections\n", len(collections))

	// 4. Create .mori directory.
	moriDir := config.MoriDirPath(opts.ProjectRoot)
	if err := config.InitDir(opts.ProjectRoot); err != nil {
		return nil, fmt.Errorf("failed to create .mori directory: %w", err)
	}

	// 5. Start shadow emulator container.
	fmt.Println("Starting Firestore emulator container...")
	mgr, err := shadow.NewManager()
	if err != nil {
		return nil, err
	}

	fmt.Println("  Pulling emulator image (this may take a moment)...")
	if err := mgr.Pull(ctx); err != nil {
		return nil, fmt.Errorf("failed to pull emulator image: %w", err)
	}

	containerInfo, err := mgr.Create(ctx, shadow.ContainerConfig{
		ProjectID: info.ProjectID,
		HostPort:  shadow.DefaultHostPort,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create emulator container: %w", err)
	}
	fmt.Printf("  Emulator: %s (port %d)\n", containerInfo.ContainerName, containerInfo.HostPort)

	// 6. Seed shadow emulator with prod data.
	if len(collections) > 0 {
		fmt.Println("Seeding shadow emulator from production...")
		shadowClient, err := newEmulatorClient(ctx, info.ProjectID, containerInfo.HostPort)
		if err != nil {
			mgr.StopAndRemove(ctx, containerInfo.ContainerID)
			return nil, fmt.Errorf("failed to connect to emulator: %w", err)
		}
		defer shadowClient.Close()

		if err := schema.SeedShadow(ctx, prodClient, shadowClient, collections, 100); err != nil {
			mgr.StopAndRemove(ctx, containerInfo.ContainerID)
			return nil, fmt.Errorf("failed to seed shadow: %w", err)
		}
		fmt.Println("  Shadow seeded.")
	}

	// 7. Persist configuration.
	fmt.Println("Persisting configuration...")
	cfg := &config.Config{
		ProdConnection:  info.Raw,
		ShadowPort:      containerInfo.HostPort,
		ShadowContainer: containerInfo.ContainerName,
		ShadowImage:     shadow.EmulatorImage,
		Engine:          "firestore",
		EngineVersion:   "emulator",
		ProxyPort:       9002,
		InitializedAt:   time.Now(),
	}

	if err := config.WriteConfig(opts.ProjectRoot, cfg); err != nil {
		mgr.StopAndRemove(ctx, containerInfo.ContainerID)
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	if err := schema.WriteTables(moriDir, collections); err != nil {
		mgr.StopAndRemove(ctx, containerInfo.ContainerID)
		return nil, fmt.Errorf("failed to write tables: %w", err)
	}

	return &InitResult{
		Config:        cfg,
		ContainerName: containerInfo.ContainerName,
		ContainerPort: containerInfo.HostPort,
		Collections:   collections,
	}, nil
}

// newFirestoreClient creates a Firestore client for the given connection info.
func newFirestoreClient(ctx context.Context, info *connstr.ConnInfo) (*firestore.Client, error) {
	var opts []option.ClientOption

	if info.IsEmulator() {
		// When using the emulator, set the environment variable and use
		// no credentials.
		os.Setenv("FIRESTORE_EMULATOR_HOST", info.EmulatorHost)
		opts = append(opts, option.WithoutAuthentication())
		opts = append(opts, option.WithEndpoint(info.EmulatorHost))
	} else if info.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(info.CredentialsFile))
	}
	// If no credentials file is provided and not emulator, ADC is used automatically.

	return firestore.NewClient(ctx, info.ProjectID, opts...)
}

// newEmulatorClient creates a Firestore client pointing at the local emulator.
func newEmulatorClient(ctx context.Context, projectID string, emulatorPort int) (*firestore.Client, error) {
	emulatorHost := shadow.EmulatorAddr(emulatorPort)
	os.Setenv("FIRESTORE_EMULATOR_HOST", emulatorHost)
	return firestore.NewClient(ctx, projectID,
		option.WithoutAuthentication(),
		option.WithEndpoint(emulatorHost),
	)
}
