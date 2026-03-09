package proxy

import (
	"context"
	"fmt"
	"os"

	firestore "cloud.google.com/go/firestore/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// sdkClients holds SDK-based Firestore clients for prod and shadow.
type sdkClients struct {
	prod   *firestore.Client
	shadow *firestore.Client
}

// newSDKClients creates Firestore SDK clients for prod and shadow backends.
// The prod client uses real credentials (or ADC), while the shadow client
// connects to the local emulator.
func newSDKClients(ctx context.Context, prodAddr, shadowAddr, credentialsFile, projectID string) (*sdkClients, error) {
	prodClient, err := dialSDKProd(ctx, prodAddr, credentialsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create prod SDK client: %w", err)
	}

	var shadowClient *firestore.Client
	if shadowAddr != "" {
		shadowClient, err = dialSDKShadow(ctx, shadowAddr, projectID)
		if err != nil {
			prodClient.Close()
			return nil, fmt.Errorf("failed to create shadow SDK client: %w", err)
		}
	}

	return &sdkClients{
		prod:   prodClient,
		shadow: shadowClient,
	}, nil
}

// close closes both SDK clients.
func (s *sdkClients) close() {
	if s.prod != nil {
		s.prod.Close()
	}
	if s.shadow != nil {
		s.shadow.Close()
	}
}

// dialSDKProd creates an SDK client connecting to production Firestore.
func dialSDKProd(ctx context.Context, prodAddr, credentialsFile string) (*firestore.Client, error) {
	var opts []option.ClientOption

	if isLocalAddr(prodAddr) {
		// Emulator-as-prod: no TLS, no auth.
		opts = append(opts,
			option.WithEndpoint(prodAddr),
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
	} else {
		// Production Firestore.
		opts = append(opts, option.WithEndpoint(prodAddr))
		if credentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(credentialsFile))
		}
		// Otherwise ADC is used automatically.
	}

	return firestore.NewClient(ctx, opts...)
}

// dialSDKShadow creates an SDK client connecting to the local emulator.
func dialSDKShadow(ctx context.Context, shadowAddr, _ string) (*firestore.Client, error) {
	// Set the emulator host environment variable for the SDK.
	os.Setenv("FIRESTORE_EMULATOR_HOST", shadowAddr)

	return firestore.NewClient(ctx,
		option.WithEndpoint(shadowAddr),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
}
