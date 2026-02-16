package mssql

import "testing"

func TestVersionToImage(t *testing.T) {
	tests := []struct {
		version string
		want    string
	}{
		{
			"Microsoft SQL Server 2022 (RTM-CU12) - 16.0.4120.1 (X64)",
			"mcr.microsoft.com/mssql/server:2022-latest",
		},
		{
			"Microsoft SQL Server 2019 (RTM-CU25) - 15.0.4355.3 (X64)",
			"mcr.microsoft.com/mssql/server:2019-latest",
		},
		{
			"Microsoft SQL Server 2017 (RTM-CU31) - 14.0.3456.2 (X64)",
			"mcr.microsoft.com/mssql/server:2017-latest",
		},
		{
			"Some Unknown Version",
			"mcr.microsoft.com/mssql/server:2022-latest",
		},
		{
			"",
			"mcr.microsoft.com/mssql/server:2022-latest",
		},
	}

	for _, tt := range tests {
		got := versionToImage(tt.version)
		if got != tt.want {
			t.Errorf("versionToImage(%q) = %q, want %q", tt.version, got, tt.want)
		}
	}
}

func TestExtractShortVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			"Microsoft SQL Server 2022 (RTM-CU12) - 16.0.4120.1 (X64)\nJan  1 2025",
			"Microsoft SQL Server 2022 (RTM-CU12) - 16.0.4120.1 (X64)",
		},
		{
			"Short",
			"Short",
		},
		{
			"",
			"",
		},
	}

	for _, tt := range tests {
		got := extractShortVersion(tt.input)
		if got != tt.want {
			t.Errorf("extractShortVersion(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
