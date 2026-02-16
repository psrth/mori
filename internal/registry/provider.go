package registry

// ProviderID uniquely identifies a hosting provider / platform.
type ProviderID string

const (
	Direct       ProviderID = "direct"
	GCPCloudSQL  ProviderID = "gcp-cloud-sql"
	AWSRDS       ProviderID = "aws-rds"
	Neon         ProviderID = "neon"
	Supabase     ProviderID = "supabase"
	Azure        ProviderID = "azure"
	PlanetScale  ProviderID = "planetscale"
	VercelPG     ProviderID = "vercel-postgres"
	MongoAtlas   ProviderID = "mongodb-atlas"
	DigitalOcean ProviderID = "digitalocean"
	Railway      ProviderID = "railway"
	Upstash      ProviderID = "upstash"
	Cloudflare   ProviderID = "cloudflare"
	Firebase     ProviderID = "firebase"
)

// ProviderTier groups providers by maturity / priority.
type ProviderTier int

const (
	ProviderTierT1 ProviderTier = 1 // Primary
	ProviderTierT2 ProviderTier = 2 // Platforms
	ProviderTierT3 ProviderTier = 3 // Serverless
)

// Provider describes a hosting platform and which engines it supports.
type Provider struct {
	ID                ProviderID
	DisplayName       string
	Tier              ProviderTier
	Category          string
	CompatibleEngines []EngineID  // which engines this provider offers
	SSLDefault        string      // override the engine's ssl default (e.g. "require")
	ExtraFields       []ConnectionField // provider-specific metadata beyond engine fields
}

var providers = []Provider{
	// ── T1: Primary ──────────────────────────────────────────────
	{
		ID: Direct, DisplayName: "Direct / Self-Hosted",
		Tier: ProviderTierT1, Category: "PRIMARY",
		CompatibleEngines: allEngineIDs(),
		// No SSL override — use engine defaults.
	},
	{
		ID: GCPCloudSQL, DisplayName: "GCP Cloud SQL",
		Tier: ProviderTierT1, Category: "PRIMARY",
		CompatibleEngines: []EngineID{Postgres, MySQL, MSSQL},
		SSLDefault:        "require",
		ExtraFields: []ConnectionField{
			{Key: "gcp_project", Label: "GCP Project", Placeholder: "my-project"},
			{Key: "gcp_instance", Label: "Instance Name", Placeholder: "my-instance"},
			{Key: "gcp_region", Label: "Region", Placeholder: "us-central1"},
		},
	},
	{
		ID: AWSRDS, DisplayName: "AWS RDS / Aurora",
		Tier: ProviderTierT1, Category: "PRIMARY",
		CompatibleEngines: []EngineID{Postgres, MySQL, MariaDB, MSSQL},
		SSLDefault:        "require",
		ExtraFields: []ConnectionField{
			{Key: "aws_region", Label: "AWS Region", Placeholder: "us-east-1"},
			{Key: "aws_instance_id", Label: "Instance Identifier", Placeholder: "my-db-instance"},
		},
	},
	{
		ID: Neon, DisplayName: "Neon",
		Tier: ProviderTierT1, Category: "PRIMARY",
		CompatibleEngines: []EngineID{Postgres},
		SSLDefault:        "require",
		ExtraFields: []ConnectionField{
			{Key: "neon_project_id", Label: "Neon Project ID", Placeholder: "autumn-forest-12345"},
			{Key: "neon_branch", Label: "Branch", Default: "main", Placeholder: "main"},
		},
	},
	{
		ID: Supabase, DisplayName: "Supabase",
		Tier: ProviderTierT1, Category: "PRIMARY",
		CompatibleEngines: []EngineID{Postgres},
		SSLDefault:        "require",
		ExtraFields: []ConnectionField{
			{Key: "supabase_project_ref", Label: "Project Reference", Placeholder: "abcdefghijklmnop"},
		},
	},

	// ── T2: Platforms ────────────────────────────────────────────
	{
		ID: Azure, DisplayName: "Azure Database",
		Tier: ProviderTierT2, Category: "PLATFORMS",
		CompatibleEngines: []EngineID{Postgres, MySQL, MariaDB, MSSQL},
		SSLDefault:        "require",
		ExtraFields: []ConnectionField{
			{Key: "azure_resource_group", Label: "Resource Group", Placeholder: "my-rg"},
			{Key: "azure_server_name", Label: "Server Name", Placeholder: "my-server"},
		},
	},
	{
		ID: PlanetScale, DisplayName: "PlanetScale",
		Tier: ProviderTierT2, Category: "PLATFORMS",
		CompatibleEngines: []EngineID{MySQL},
		SSLDefault:        "require",
		ExtraFields: []ConnectionField{
			{Key: "ps_organization", Label: "Organization", Placeholder: "my-org"},
		},
	},
	{
		ID: VercelPG, DisplayName: "Vercel Postgres",
		Tier: ProviderTierT2, Category: "PLATFORMS",
		CompatibleEngines: []EngineID{Postgres},
		SSLDefault:        "require",
	},
	{
		ID: MongoAtlas, DisplayName: "MongoDB Atlas",
		Tier: ProviderTierT2, Category: "PLATFORMS",
		CompatibleEngines: []EngineID{MongoDB},
		SSLDefault:        "true",
		ExtraFields: []ConnectionField{
			{Key: "atlas_cluster", Label: "Cluster Name", Placeholder: "cluster0"},
		},
	},
	{
		ID: DigitalOcean, DisplayName: "DigitalOcean Managed DB",
		Tier: ProviderTierT2, Category: "PLATFORMS",
		CompatibleEngines: []EngineID{Postgres, MySQL, Redis, MongoDB},
		SSLDefault:        "require",
		ExtraFields: []ConnectionField{
			{Key: "do_cluster_id", Label: "Cluster ID", Placeholder: "abc-123"},
		},
	},

	// ── T3: Serverless ───────────────────────────────────────────
	{
		ID: Railway, DisplayName: "Railway",
		Tier: ProviderTierT3, Category: "SERVERLESS",
		CompatibleEngines: []EngineID{Postgres, MySQL, Redis, MongoDB},
		SSLDefault:        "require",
		ExtraFields: []ConnectionField{
			{Key: "railway_project_id", Label: "Project ID", Placeholder: "abc-123"},
		},
	},
	{
		ID: Upstash, DisplayName: "Upstash",
		Tier: ProviderTierT3, Category: "SERVERLESS",
		CompatibleEngines: []EngineID{Redis},
		SSLDefault:        "true",
	},
	{
		ID: Cloudflare, DisplayName: "Cloudflare D1 / KV",
		Tier: ProviderTierT3, Category: "SERVERLESS",
		CompatibleEngines: []EngineID{SQLite, Redis},
		ExtraFields: []ConnectionField{
			{Key: "cf_account_id", Label: "Account ID", Required: true, Placeholder: "abc123", Validate: validateNonEmpty("account ID")},
			{Key: "cf_database_id", Label: "Database ID", Required: true, Placeholder: "def456", Validate: validateNonEmpty("database ID")},
		},
	},
	{
		ID: Firebase, DisplayName: "Firebase / Firestore",
		Tier: ProviderTierT3, Category: "SERVERLESS",
		CompatibleEngines: []EngineID{Firestore},
	},
}

// AllProviders returns every registered provider in tier order.
func AllProviders() []Provider {
	out := make([]Provider, len(providers))
	copy(out, providers)
	return out
}

// ProviderByID looks up a provider by its ID. Returns false if not found.
func ProviderByID(id ProviderID) (Provider, bool) {
	for _, p := range providers {
		if p.ID == id {
			return p, true
		}
	}
	return Provider{}, false
}

// ProvidersForEngine returns providers that are compatible with the given engine.
func ProvidersForEngine(engineID EngineID) []Provider {
	var out []Provider
	for _, p := range providers {
		for _, eid := range p.CompatibleEngines {
			if eid == engineID {
				out = append(out, p)
				break
			}
		}
	}
	return out
}

// ProviderTierLabel returns a human-readable label for a provider tier.
func ProviderTierLabel(t ProviderTier) string {
	switch t {
	case ProviderTierT1:
		return "Primary"
	case ProviderTierT2:
		return "Platforms"
	case ProviderTierT3:
		return "Serverless"
	default:
		return "Unknown"
	}
}

// allEngineIDs returns the IDs of every registered engine.
// Used by the "Direct / Self-Hosted" provider which is compatible with everything.
func allEngineIDs() []EngineID {
	ids := make([]EngineID, len(engines))
	for i, e := range engines {
		ids[i] = e.ID
	}
	return ids
}
