package quota

import (
	"strings"
	"testing"
)

func TestParseConfig_Full(t *testing.T) {
	raw := `{
		"version":"1.2",
		"defaultWeight":3,
		"degrade":"allow",
		"globalMaxConcurrent":42,
		"rules":[
			{"tenant":"a","api":"/v1/x","rate":2.5,"burst":7,"maxConcurrent":9,"weight":11},
			{"tenant":"a","rate":1,"burst":2,"maxConcurrent":3}
		]
	}`
	cfg, err := ParseConfig(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Version != "1.2" || cfg.DefaultWeight != 3 || cfg.Degrade != DegradeAllow {
		t.Fatalf("top-level mismatch: %+v", cfg)
	}
	if cfg.GlobalMaxConcurrent != 42 {
		t.Fatalf("shared=%d want 42", cfg.GlobalMaxConcurrent)
	}
	r, ok := cfg.RuleFor("a", "/v1/x")
	if !ok || r.Rate != 2.5 || r.Burst != 7 || r.MaxConcurrent != 9 || r.Weight != 11 {
		t.Fatalf("rule mismatch: %+v", r)
	}
}

func TestParseConfig_DefaultDegradeDeny(t *testing.T) {
	cfg, err := ParseConfig(strings.NewReader(`{"version":"1.0","rules":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Degrade != DegradeDeny {
		t.Fatalf("default degrade=%v want deny (fail-closed)", cfg.Degrade)
	}
}

func TestParseConfig_Invalid(t *testing.T) {
	cases := map[string]string{
		"bad json":           `{not json`,
		"unknown field":      `{"version":"1.0","bogus":1}`,
		"bad degrade":        `{"version":"1.0","degrade":"explode"}`,
		"incompatible major": `{"version":"3.0"}`,
		"rule without burst": `{"version":"1.0","rules":[{"tenant":"a","rate":5}]}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig(strings.NewReader(raw)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestParseDegradePolicy(t *testing.T) {
	for s, want := range map[string]DegradePolicy{
		"": DegradeDeny, "deny": DegradeDeny, "allow": DegradeAllow,
	} {
		got, err := ParseDegradePolicy(s)
		if err != nil || got != want {
			t.Fatalf("ParseDegradePolicy(%q)=%v,%v want %v", s, got, err, want)
		}
	}
	if _, err := ParseDegradePolicy("nope"); err == nil {
		t.Fatal("expected error")
	}
}

// 旧配置（无 concurrencyTTLSeconds 字段）继续解析通过，值为 0 表示用默认。
func TestParseConfig_LegacyWithoutTTLStillWorks(t *testing.T) {
	raw := `{
		"version":"1.0",
		"degrade":"deny",
		"globalMaxConcurrent":42,
		"rules":[{"tenant":"a","rate":2,"burst":7,"maxConcurrent":9}]
	}`
	cfg, err := ParseConfig(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("legacy config must still parse: %v", err)
	}
	if cfg.ConcurrencyTTLSeconds != 0 {
		t.Fatalf("ttl seconds=%d want 0 (use default)", cfg.ConcurrencyTTLSeconds)
	}
}

// 新字段解析与边界校验。
func TestParseConfig_ConcurrencyTTL(t *testing.T) {
	cfg, err := ParseConfig(strings.NewReader(
		`{"version":"1.3","concurrencyTTLSeconds":15,"rules":[]}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.ConcurrencyTTLSeconds != 15 {
		t.Fatalf("ttl=%d want 15", cfg.ConcurrencyTTLSeconds)
	}
	for _, raw := range []string{
		`{"version":"1.0","concurrencyTTLSeconds":-1}`,
		`{"version":"1.0","concurrencyTTLSeconds":3601}`,
	} {
		if _, err := ParseConfig(strings.NewReader(raw)); err == nil {
			t.Fatalf("invalid TTL config must be rejected: %s", raw)
		}
	}
}
