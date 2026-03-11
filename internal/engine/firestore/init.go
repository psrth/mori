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
	"github.com/mori-dev/mori/internal/ui"
	"github.com/mori-dev/mori/internal/engine/firestore/schema"
	"github.com/mori-dev/mori/internal/engine/firestore/shadow"
)

// InitOptions holds the options for initializing a Firestore Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
	ConnName    string
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
	var prodClient *firestore.Client
	if err := ui.Spinner("Connecting to production Firestore...", func() error {
		var e error
		prodClient, e = newFirestoreClient(ctx, info)
		return e
	}); err != nil {
		return nil, fmt.Errorf("cannot connect to Firestore project %q: %w", info.ProjectID, err)
	}
	defer prodClient.Close()

	// 3. Detect collections.
	var collections map[string]schema.CollectionMeta
	if err := ui.Spinner("Detecting collections...", func() error {
		var e error
		collections, e = schema.DetectCollections(ctx, prodClient)
		return e
	}); err != nil {
		return nil, fmt.Errorf("collection detection failed: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Connected — %d collections", len(collections)))

	// 4. Create .mori directory.
	moriDir := config.ConnDir(opts.ProjectRoot, opts.ConnName)
	if err := config.InitConnDir(opts.ProjectRoot, opts.ConnName); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	// 5. Start shadow emulator container.
	mgr, err := shadow.NewManager()
	if err != nil {
		return nil, err
	}

	if err := ui.Spinner("Pulling Firestore emulator image...", func() error {
		return mgr.Pull(ctx)
	}); err != nil {
		return nil, fmt.Errorf("failed to pull emulator image: %w", err)
	}

	var containerInfo *shadow.ContainerInfo
	if err := ui.Spinner("Creating Firestore emulator container...", func() error {
		var e error
		containerInfo, e = mgr.Create(ctx, shadow.ContainerConfig{
			ProjectID: info.ProjectID,
			HostPort:  shadow.DefaultHostPort,
		})
		return e
	}); err != nil {
		return nil, fmt.Errorf("failed to create emulator container: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Emulator ready on port %d", containerInfo.HostPort))

	// 6. Seed shadow emulator with prod data (including subcollections).
	if len(collections) > 0 {
		if err := ui.Spinner("Seeding shadow emulator from production (including subcollections)...", func() error {
			shadowClient, e := newEmulatorClient(ctx, info.ProjectID, containerInfo.HostPort)
			if e != nil {
				return e
			}
			defer shadowClient.Close()
			return schema.SeedShadowWithSubcollections(ctx, prodClient, shadowClient, collections, schema.DefaultSeedLimit, schema.DefaultMaxSeedDepth)
		}); err != nil {
			mgr.StopAndRemove(ctx, containerInfo.ContainerID)
			return nil, fmt.Errorf("failed to seed shadow: %w", err)
		}
		ui.StepDone("Shadow seeded")
	}

	// 7. Persist configuration.
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

	if err := config.WriteConnConfig(opts.ProjectRoot, opts.ConnName, cfg); err != nil {
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
