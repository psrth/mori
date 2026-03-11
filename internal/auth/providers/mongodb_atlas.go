package providers

import (
	"context"
	"fmt"
	"net/url"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func init() { auth.Register(&mongoAtlasProvider{}) }

type mongoAtlasProvider struct{}

func (p *mongoAtlasProvider) ID() registry.ProviderID { return registry.MongoAtlas }

func (p *mongoAtlasProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *mongoAtlasProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	host := conn.Host
	if host == "" {
		cluster := conn.Extra["atlas_cluster"]
		if cluster == "" {
			return "", fmt.Errorf("mongodb-atlas: host or atlas_cluster is required")
		}
		host = cluster + ".mongodb.net"
	}

	// SRV connections omit the port.
	u := &url.URL{
		Scheme: "mongodb+srv",
		User:   url.UserPassword(conn.User, conn.Password),
		Host:   host,
		Path:   conn.Database,
	}
	q := u.Query()
	q.Set("retryWrites", "true")
	q.Set("w", "majority")
	u.RawQuery = q.Encode()

	return u.String(), nil
}
