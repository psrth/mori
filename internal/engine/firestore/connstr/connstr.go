package connstr

import (
	"fmt"
	"net/url"
	"strings"
)

// ConnInfo holds parsed Firestore connection parameters.
type ConnInfo struct {
	ProjectID       string // GCP project ID
	CredentialsFile string // Path to service account JSON (optional)
	DatabaseID      string // Firestore database ID (default: "(default)")
	EmulatorHost    string // If set, use emulator instead of prod
	Raw             string // Original connection string
}

// Parse accepts a Firestore connection string and returns ConnInfo.
// Supported formats:
//   - firestore://project-id
//   - firestore://project-id?credentials=/path/to/creds.json
//   - firestore://project-id?database=my-db
//   - project-id (plain project ID string)
//   - project-id?credentials=/path/to/creds.json
func Parse(connStr string) (*ConnInfo, error) {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return nil, fmt.Errorf("connection string is empty")
	}

	info := &ConnInfo{
		Raw:        connStr,
		DatabaseID: "(default)",
	}

	if strings.HasPrefix(connStr, "firestore://") {
		if err := parseFirestoreURI(connStr, info); err != nil {
			return nil, err
		}
	} else if strings.Contains(connStr, "?") {
		// project-id?credentials=/path/to/creds.json
		parts := strings.SplitN(connStr, "?", 2)
		info.ProjectID = parts[0]
		if err := parseQueryParams(parts[1], info); err != nil {
			return nil, err
		}
	} else {
		info.ProjectID = connStr
	}

	if info.ProjectID == "" {
		return nil, fmt.Errorf("project ID is required")
	}

	return info, nil
}

func parseFirestoreURI(connStr string, info *ConnInfo) error {
	u, err := url.Parse(connStr)
	if err != nil {
		return fmt.Errorf("invalid firestore URI: %w", err)
	}

	// The host portion is the project ID.
	info.ProjectID = u.Host
	if info.ProjectID == "" {
		// Try opaque form: firestore:project-id
		info.ProjectID = u.Opaque
	}
	if info.ProjectID == "" {
		return fmt.Errorf("firestore URI missing project ID")
	}

	for k, vals := range u.Query() {
		if len(vals) == 0 {
			continue
		}
		switch k {
		case "credentials", "credentials_file":
			info.CredentialsFile = vals[0]
		case "database", "database_id":
			info.DatabaseID = vals[0]
		case "emulator_host":
			info.EmulatorHost = vals[0]
		}
	}

	return nil
}

func parseQueryParams(query string, info *ConnInfo) error {
	vals, err := url.ParseQuery(query)
	if err != nil {
		return fmt.Errorf("invalid query parameters: %w", err)
	}
	for k, v := range vals {
		if len(v) == 0 {
			continue
		}
		switch k {
		case "credentials", "credentials_file":
			info.CredentialsFile = v[0]
		case "database", "database_id":
			info.DatabaseID = v[0]
		case "emulator_host":
			info.EmulatorHost = v[0]
		}
	}
	return nil
}

// ResourceName returns the Firestore database resource name.
func (c *ConnInfo) ResourceName() string {
	return fmt.Sprintf("projects/%s/databases/%s", c.ProjectID, c.DatabaseID)
}

// ProdAddr returns the production Firestore endpoint.
func (c *ConnInfo) ProdAddr() string {
	if c.EmulatorHost != "" {
		return c.EmulatorHost
	}
	return "firestore.googleapis.com:443"
}

// IsEmulator reports whether this connection targets an emulator.
func (c *ConnInfo) IsEmulator() bool {
	return c.EmulatorHost != ""
}
