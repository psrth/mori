package registry

// FieldsWithProviderDefaults returns the connection fields for an engine
// with provider-specific defaults applied (e.g. SSL mode overrides).
// Provider extra fields are appended at the end.
func FieldsWithProviderDefaults(engineID EngineID, providerID ProviderID) []ConnectionField {
	base := FieldsForEngine(engineID)
	if base == nil {
		return nil
	}

	provider, ok := ProviderByID(providerID)
	if !ok {
		return base
	}

	// Apply SSL default override from provider.
	if provider.SSLDefault != "" {
		for i := range base {
			if base[i].Key == "ssl_mode" || base[i].Key == "ssl" {
				base[i].Default = provider.SSLDefault
			}
		}
	}

	// Append provider-specific extra fields.
	base = append(base, provider.ExtraFields...)

	return base
}

// IsEngineSupported returns true if the engine can be proxied today.
func IsEngineSupported(id EngineID) bool {
	e, ok := EngineByID(id)
	return ok && e.Supported
}

// IsProviderCompatible returns true if the provider supports the given engine.
func IsProviderCompatible(providerID ProviderID, engineID EngineID) bool {
	p, ok := ProviderByID(providerID)
	if !ok {
		return false
	}
	for _, eid := range p.CompatibleEngines {
		if eid == engineID {
			return true
		}
	}
	return false
}
