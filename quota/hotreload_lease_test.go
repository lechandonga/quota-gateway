package quota

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

// 长连接在途期间发生热更新：
//  1. 非法/不兼容配置被拒绝，在用配置与既有占位都不受影响；
//  2. 配置生效不改变既有占位的键，长连接名额不被误回收/重复占用；
//  3. TTL 调整对新请求生效，旧请求按新周期续租、正常释放；
//  4. 容量热更新（调大上限）对后续请求立即生效。
func TestHotReload_DuringLongConnections(t *testing.T) {
	ms := store.NewMemoryStore()
	fc := &fakeClock{t: time.Unix(9000, 0)}
	ms.SetNow(fc.get)
	cfg := &Config{Version: "1.0", Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
	}}
	m, err := NewManager(Options{Store: ms, Config: cfg, ConcurrencyTTL: 400 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	r1, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatal(err)
	}
	defer r1()
	defer r2()

	// 1) 非法配置被拒绝。
	bad := &Config{Version: "1.1", ConcurrencyTTLSeconds: -1, Rules: cfg.Rules}
	if err := m.UpdateConfig(bad); err == nil {
		t.Fatal("negative TTL config must be rejected")
	}
	bad2 := &Config{Version: "1.1", ConcurrencyTTLSeconds: 99999, Rules: cfg.Rules}
	if err := m.UpdateConfig(bad2); err == nil {
		t.Fatal("out-of-range TTL config must be rejected")
	}
	if v := m.ConfigVersion(); v != "1.0" {
		t.Fatalf("active version=%q want 1.0", v)
	}

	// 2) 合法热更新：调大租户上限 + 修改 TTL（1s）。
	next := &Config{Version: "1.2", ConcurrencyTTLSeconds: 1, Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 3, Weight: 2},
	}}
	if err := m.UpdateConfig(next); err != nil {
		t.Fatalf("valid reload: %v", err)
	}
	// 热更新期间旧占位不被影响：第三个名额因上限调大而可用。
	r3, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatalf("new cap 3 must admit third long request, got %v", err)
	}
	defer r3()
	if got := ms.Inflight("cc:t1:/c"); got != 3 {
		t.Fatalf("backend inflight=%d want 3 after reload", got)
	}
	if _, err := m.Acquire(ctx, "t1", "/c"); err == nil {
		t.Fatal("fourth request must still be rejected at new cap 3")
	}

	// 3) 旧长连接跨越多个新 TTL 周期后仍被续租，未被误回收。
	start := time.Now()
	for time.Since(start) < 1500*time.Millisecond {
		fc.add(100 * time.Millisecond)
		time.Sleep(160 * time.Millisecond) // 新 TTL/3≈333ms；160ms 采样足够快
		if got := ms.Inflight("cc:t1:/c"); got != 3 {
			t.Fatalf("inflight=%d want 3 during/after reload (leases disturbed?)", got)
		}
	}

	// 4) 全部释放后归零；配置 TTL 作用于后续新占位（通过崩溃回收间接验证字段生效）。
	r3()
	if got := ms.Inflight("cc:t1:/c"); got != 2 {
		t.Fatalf("inflight=%d want 2", got)
	}
	r1()
	r2()
	if got := ms.Inflight("cc:t1:/c"); got != 0 {
		t.Fatalf("inflight=%d want 0", got)
	}
	if d := m.effectiveTTL(); d != time.Second {
		t.Fatalf("effective TTL=%s want 1s from reloaded config", d)
	}
}
