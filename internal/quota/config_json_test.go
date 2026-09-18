package quota

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestExampleConfigLoadsAndValidates(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "example.json")
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("example config not found: %v", err)
	}
	defer f.Close()
	var cfg Config
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		t.Fatalf("strict decode failed: %v", err)
	}
	if err := ValidateConfig(&cfg); err != nil {
		t.Fatalf("example config must be valid: %v", err)
	}
	if cfg.APIVersion != APIVersion || cfg.Mode != ModeDistributed {
		t.Fatalf("unexpected parsed config: api=%s mode=%s", cfg.APIVersion, cfg.Mode)
	}
	g, ok := cfg.Groups["llm-pool"]
	if !ok || g.Capacity != 40 {
		t.Fatalf("group not parsed: %+v", cfg.Groups)
	}
}

func TestConfigJSONRoundTrip(t *testing.T) {
	cfg := baseConfig()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var back Config
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfig(&back); err != nil {
		t.Fatalf("round-tripped config invalid: %v", err)
	}
	if back.Tenants["t1"]["api"].Burst != 5 {
		t.Fatalf("burst lost in round trip: %+v", back.Tenants)
	}
}
