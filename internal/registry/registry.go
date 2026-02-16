package registry

// FieldsWithProviderDefaults returns the connection fields for an engine
// with provider-specific defaults applied (e.g. SSL mode, host placeholder,
// port, database, and user overrides). Provider extra fields are appended
// at the end.
func FieldsWithProviderDefaults(engineID EngineID, providerID ProviderID) []ConnectionField {
	base := FieldsForEngine(engineID)
	if base == nil {
		return nil
	}

	provider, ok := ProviderByID(providerID)
	if !ok {
		return base
	}

	for i := range base {
		switch base[i].Key {
		case "ssl_mode", "ssl":
			if provider.SSLDefault != "" {
				base[i].Default = provider.SSLDefault
			}
		case "host":
			if provider.HostPlaceholder != "" {
				base[i].Placeholder = provider.HostPlaceholder
			}
		case "port":
			if provider.PortDefault != "" {
				base[i].Default = provider.PortDefault
				base[i].Placeholder = provider.PortDefault
			}
		case "database":
			if provider.DatabaseDefault != "" {
				base[i].Default = provider.DatabaseDefault
				base[i].Placeholder = provider.DatabaseDefault
			}
		case "user":
			if provider.UserDefault != "" {
				base[i].Default = provider.UserDefault
				base[i].Placeholder = provider.UserDefault
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
