package quota

import (
	"strings"
	"testing"
)

func TestValidate_AcceptsValidConfig(t *testing.T) {
	cfg := &Config{
		Version:       "1.0",
		DefaultWeight: 2,
		Degrade:       DegradeDeny,
		Rules: []Rule{
			{Tenant: "a", API: "/v1/chat", Rate: 10, Burst: 20, MaxConcurrent: 5, Weight: 3},
			{Tenant: "a", Rate: 1, Burst: 1}, // 租户默认规则
		},
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}

func TestValidate_RejectsBadRules(t *testing.T) {
	cases := []struct {
		name string
		cfg  *Config
		want string
	}{
		{"nil", nil, "nil"},
		{"empty version", &Config{Version: ""}, "version"},
		{"bad version", &Config{Version: "v1"}, "malformed"},
		{"major mismatch", &Config{Version: "2.0"}, "incompatible"},
		{"too old", &Config{Version: "1.0-beta"}, "malformed"},
		{"empty tenant", &Config{Version: "1.0", Rules: []Rule{{Tenant: ""}}}, "tenant"},
		{"negative rate", &Config{Version: "1.0", Rules: []Rule{{Tenant: "a", Rate: -1}}}, "rate"},
		{"burst without rate", &Config{Version: "1.0", Rules: []Rule{{Tenant: "a", Rate: 1, Burst: 0}}}, "burst"},
		{"negative concurrency", &Config{Version: "1.0", Rules: []Rule{{Tenant: "a", MaxConcurrent: -1}}}, "maxConcurrent"},
		{"duplicate rule", &Config{Version: "1.0", Rules: []Rule{
			{Tenant: "a", API: "/x", Burst: 1}, {Tenant: "a", API: "/x", Burst: 1},
		}}, "duplicate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.cfg)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
			if !isConfigError(err) {
				t.Fatalf("expected configError, got %T: %v", err, err)
			}
		})
	}
}

func TestRuleFor_PrefersExactThenFallback(t *testing.T) {
	cfg := &Config{Version: "1.0", DefaultWeight: 1, Rules: []Rule{
		{Tenant: "a", API: "/x", Burst: 1, Weight: 9},
		{Tenant: "a", API: "", Rate: 5, Burst: 5, Weight: 7},
	}}
	got := cfg.normalize()

	r, ok := got.RuleFor("a", "/x")
	if !ok || r.Weight != 9 {
		t.Fatalf("exact match failed: %+v ok=%v", r, ok)
	}
	r, ok = got.RuleFor("a", "/other")
	if !ok || r.Rate != 5 || r.API != "" {
		t.Fatalf("fallback match failed: %+v ok=%v", r, ok)
	}
	if _, ok := got.RuleFor("b", "/x"); ok {
		t.Fatalf("unknown tenant should not match")
	}
}

func TestNormalize_FillsDefaultWeight(t *testing.T) {
	cfg := (&Config{Version: "1.0", DefaultWeight: 4, Rules: []Rule{
		{Tenant: "a", Burst: 1, Weight: 0},
	}}).normalize()
	if cfg.Rules[0].Weight != 4 {
		t.Fatalf("weight = %d, want 4", cfg.Rules[0].Weight)
	}
}

func TestParseConfig_RejectsUnknownFields(t *testing.T) {
	raw := `{"version":"1.0","rules":[{"tenant":"a","rate":1,"burst":1,"mystery":1}]}`
	if _, err := ParseConfig(strings.NewReader(raw)); err == nil {
		t.Fatal("expected unknown field rejection")
	}
}
