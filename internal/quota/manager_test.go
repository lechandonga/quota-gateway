package quota

import (
	"context"
	"errors"
	"testing"
)

func baseConfig() *Config {
	return &Config{
		APIVersion: APIVersion,
		Version:    1,
		Mode:       ModeLocal,
		Degrade:    DegradeFailClosed,
		Tenants: map[string]map[string]TenantLimit{
			"t1": {
				"api": {RatePerSec: 10, Burst: 5, MaxConcurrent: 3, Weight: 1},
			},
		},
		Groups: map[string]Group{
			"shared": {Name: "shared", Capacity: 10, DefaultWeight: 1},
		},
		TenantGroups: map[string]map[string]string{
			"t1": {"api": "shared"},
		},
	}
}

func TestValidateConfigAccepts(t *testing.T) {
	if err := ValidateConfig(baseConfig()); err != nil {
		t.Fatalf("expected valid: %v", err)
	}
}

func TestValidateConfigRejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(c *Config)
	}{
		{"bad api version", func(c *Config) { c.APIVersion = "v999" }},
		{"zero rate", func(c *Config) { c.Tenants["t1"]["api"] = TenantLimit{RatePerSec: 0, Burst: 1} }},
		{"zero burst", func(c *Config) { c.Tenants["t1"]["api"] = TenantLimit{RatePerSec: 1, Burst: 0} }},
		{"negative concurrency", func(c *Config) { l := c.Tenants["t1"]["api"]; l.MaxConcurrent = -1; c.Tenants["t1"]["api"] = l }},
		{"unknown mode", func(c *Config) { c.Mode = "weird" }},
		{"bad degrade", func(c *Config) { c.Degrade = "nope" }},
		{"dangling group ref", func(c *Config) { c.TenantGroups["t1"]["api"] = "missing" }},
		{"zero group capacity", func(c *Config) { g := c.Groups["shared"]; g.Capacity = 0; c.Groups["shared"] = g }},
		{"empty tenant", func(c *Config) { c.Tenants[""] = map[string]TenantLimit{} }},
		{"heartbeat not below ttl", func(c *Config) {
			c.Mode = ModeDistributed
			c.SessionTTLMillis = 100
			c.HeartbeatMillis = 100
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConfig()
			tc.mut(c)
			err := ValidateConfig(c)
			if err == nil {
				t.Fatalf("expected validation error")
			}
			var qe *QuotaError
			if !errors.As(err, &qe) || qe.Reason != ReasonBadConfig {
				t.Fatalf("expected bad_config quota error, got %v", err)
			}
		})
	}
}

func TestManagerReplaceVersionRegression(t *testing.T) {
	m, err := NewManager(baseConfig(), SystemClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	older := baseConfig()
	older.Version = 0
	if err := m.Replace(context.Background(), older); err == nil {
		t.Fatal("expected regression rejection")
	}
	if m.Current().Version != 1 {
		t.Fatalf("current config must remain v1, got %d", m.Current().Version)
	}
	newer := baseConfig()
	newer.Version = 2
	if err := m.Replace(context.Background(), newer); err != nil {
		t.Fatalf("expected upgrade accepted: %v", err)
	}
	if m.Current().Version != 2 {
		t.Fatalf("expected v2, got %d", m.Current().Version)
	}
}

func TestManagerReplaceInvalidKeepsCurrent(t *testing.T) {
	m, _ := NewManager(baseConfig(), SystemClock(), nil)
	bad := baseConfig()
	bad.Version = 3
	bad.Degrade = "bogus"
	if err := m.Replace(context.Background(), bad); err == nil {
		t.Fatal("expected rejection")
	}
	if m.Current().Version != 1 {
		t.Fatalf("invalid replace must not affect current, got v%d", m.Current().Version)
	}
}
