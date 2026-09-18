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
