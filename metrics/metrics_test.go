package metrics

import (
	"testing"
	"time"
)

func TestRecorder_CountersAndSnapshot(t *testing.T) {
	m := newTestRecorder()

	m.IncRequests("a", "/x")
	m.IncRequests("a", "/x")
	m.IncAllowed("a", "/x")
	m.IncRejected("a", "/x", "rate_limited")
	m.IncInflight("a", "/x")
	m.IncInflight("a", "/x")
	m.DecInflight("a", "/x")
	m.SetStoreHealthy(false)
	m.SetConfigVersion("1.3")
	m.ObserveLatency("a", "/x", 5*time.Millisecond)
	m.ObserveLatency("a", "/x", 9*time.Millisecond)

	snap := m.Snapshot()
	if snap["requests_total"].(int64) != 2 {
		t.Fatalf("requests_total=%v", snap["requests_total"])
	}
	if snap["allowed_total"].(int64) != 1 || snap["rejected_total"].(int64) != 1 {
		t.Fatalf("totals wrong: %v %v", snap["allowed_total"], snap["rejected_total"])
	}
	if snap["store_healthy"].(bool) {
		t.Fatalf("store_healthy should be false")
	}
	if snap["config_version"].(string) != "1.3" {
		t.Fatalf("config_version=%v", snap["config_version"])
	}
	inflight := snap["inflight_by_key"].(map[string]int64)
	if inflight["a|/x"] != 1 {
		t.Fatalf("inflight=%d want 1", inflight["a|/x"])
	}
	lat := snap["latency_by_key_us"].(map[string]map[string]int64)["a|/x"]
	if lat["count"] != 2 || lat["max_us"] < 9000 {
		t.Fatalf("latency stats wrong: %+v", lat)
	}
}

func TestRecorder_DegradedCounter(t *testing.T) {
	m := newTestRecorder()
	m.IncDegraded("a", "/x", "allow")
	if m.Snapshot()["degraded_total"].(int64) != 1 {
		t.Fatal("degraded_total mismatch")
	}
}
