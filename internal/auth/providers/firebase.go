package providers

import (
	"context"
	"fmt"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func init() { auth.Register(&firebaseProvider{}) }

type firebaseProvider struct{}

func (p *firebaseProvider) ID() registry.ProviderID { return registry.Firebase }

func (p *firebaseProvider) Fields(engine registry.EngineID) []registry.ConnectionField {
	return []registry.ConnectionField{
		{Key: "project_id", Label: "GCP Project ID", Required: true, Placeholder: "my-project"},
		{Key: "credentials_file", Label: "Credentials JSON Path", Placeholder: "./service-account.json"},
	}
}

func (p *firebaseProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	projectID := conn.Extra["project_id"]
	if projectID == "" {
		projectID = conn.Database
	}
	if projectID == "" {
		return "", fmt.Errorf("firebase: project_id or database is required")
	}

	connStr := fmt.Sprintf("firestore://%s", projectID)
	if creds := conn.Extra["credentials_file"]; creds != "" {
		connStr += "?credentials=" + creds
	}
	return connStr, nil
}
