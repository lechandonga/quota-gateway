package quota

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func distributedConfig() *Config {
	return &Config{
		APIVersion: APIVersion,
		Version:    1,
		Mode:       ModeDistributed,
		Degrade:    DegradeFailClosed,
		Tenants: map[string]map[string]TenantLimit{
			"ta": {"api": {RatePerSec: 100, Burst: 100, MaxConcurrent: 4, Weight: 1}},
			"tb": {"api": {RatePerSec: 100, Burst: 100, MaxConcurrent: 0, Weight: 1}},
		},
		Groups: map[string]Group{
			"shared": {Name: "shared", Capacity: 6, DefaultWeight: 1},
		},
		TenantGroups: map[string]map[string]string{
			"ta": {"api": "shared"},
			"tb": {"api": "shared"},
		},
		SessionTTLMillis:     500,
		HeartbeatMillis:      100,
		RequestTimeoutMillis: 1000,
	}
}

// startCoordinator 启动一个真实 HTTP 协调服务（httptest），返回 baseURL 与关闭函数。
func startCoordinator(t *testing.T, cfg *Config) (string, *MemoryStore, func()) {
	t.Helper()
	store := NewMemoryStore(SystemClock())
	if err := store.SyncConfig(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	srv := NewCoordinatorServer(store, SystemClock())
	ts := httptest.NewServer(srv.Handler())
	return ts.URL, store, func() {
		ts.Close()
		_ = store.Close()
	}
}

func newRemoteGateway(t *testing.T, cfg *Config, baseURL, id string) *Gateway {
	t.Helper()
	g, err := NewGateway(cfg, Options{
		Mode:       ModeDistributed,
		BaseURL:    baseURL,
		InstanceID: id,
		Degrade:    cfg.Degrade,
		MaxWait:    200 * time.Millisecond,
		SessionTTL: 500 * time.Millisecond,
		Heartbeat:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("gateway %s: %v", id, err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestDistributedHardCapAcrossInstances(t *testing.T) {
	cfg := distributedConfig()
	baseURL, _, shutdown := startCoordinator(t, cfg)
	defer shutdown()
	g1 := newRemoteGateway(t, cfg, baseURL, "inst-1")
	g2 := newRemoteGateway(t, cfg, baseURL, "inst-2")

	const perInstance = 16
	var granted int64
	var rejected int64
	var mu sync.Mutex
	var leases []*Lease
	var lmu sync.Mutex

	var wg sync.WaitGroup
	try := func(g *Gateway) {
		defer wg.Done()
		l, err := g.Acquire(context.Background(), "ta", "api")
		mu.Lock()
		if err != nil {
			rejected++
		} else {
			granted++
			lmu.Lock()
			leases = append(leases, l)
			lmu.Unlock()
		}
		mu.Unlock()
	}
	for i := 0; i < perInstance; i++ {
		wg.Add(2)
		go try(g1)
		go try(g2)
	}
	wg.Wait()

	if granted > 4 {
		t.Fatalf("cluster-wide active grants must not exceed hard cap 4, got %d", granted)
	}
	if granted+rejected != 2*perInstance {
		t.Fatalf("every request decided exactly once: granted=%d rejected=%d", granted, rejected)
	}
	for _, l := range leases {
		l.Release()
	}
}

func TestDistributedSlotsRecoveredAfterRelease(t *testing.T) {
	cfg := distributedConfig()
	baseURL, store, shutdown := startCoordinator(t, cfg)
	defer shutdown()
	g1 := newRemoteGateway(t, cfg, baseURL, "inst-a")
	g2 := newRemoteGateway(t, cfg, baseURL, "inst-b")

	first, err := g1.Acquire(context.Background(), "ta", "api")
	if err != nil {
		t.Fatal(err)
	}
	second, err := g2.Acquire(context.Background(), "ta", "api")
	if err != nil {
		t.Fatal(err)
	}
	// 两个实例各自持有，占用一致
	if got := store.active["ta\x00api"]; got != 2 {
		t.Fatalf("expected 2 cluster-wide active, got %d", got)
	}
	first.Release()
	second.Release()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.leases) == 0 && len(store.active) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("slots not released: leases=%d active=%v", len(store.leases), store.active)
}

// TestDistributedInstanceCrashReapsLeases 验证实例崩溃（租约永不释放）时，
// 协调端凭会话 TTL 回收占用，额度不泄漏、不放大。
func TestDistributedInstanceCrashReapsLeases(t *testing.T) {
	cfg := distributedConfig()
	cfg.SessionTTLMillis = 400
	baseURL, store, shutdown := startCoordinator(t, cfg)
	defer shutdown()

	g1 := newRemoteGateway(t, cfg, baseURL, "crashed-inst")
	l, err := g1.Acquire(context.Background(), "ta", "api")
	if err != nil {
		t.Fatal(err)
	}
	_ = l
	if got := store.active["ta\x00api"]; got != 1 {
		t.Fatalf("expected active 1, got %d", got)
	}
	// 实例“崩溃”：直接关闭且不调用 Release
	_ = g1.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		n := store.active["ta\x00api"]
		leases := len(store.leases)
		store.mu.Unlock()
		if n == 0 && leases == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("leases must be reaped after session TTL: active=%v leases=%d",
		store.active, len(store.leases))
}

// TestDistributedCoordinatorRejectConfig 验证协调端拒绝非法配置且不影响线上。
func TestDistributedCoordinatorRejectConfig(t *testing.T) {
	cfg := distributedConfig()
	baseURL, _, shutdown := startCoordinator(t, cfg)
	defer shutdown()
	g := newRemoteGateway(t, cfg, baseURL, "cfg-inst")

	bad := distributedConfig()
	bad.Version = 2
	bad.Degrade = "totally-bogus"
	if err := g.ReplaceConfig(context.Background(), bad); err == nil {
		t.Fatal("invalid config must be rejected")
	}
	if got := g.CurrentConfig().Version; got != 1 {
		t.Fatalf("running config must stay v1, got %d", got)
	}
	// 合法新版本应生效
	good := distributedConfig()
	good.Version = 3
	l := g.CurrentConfig().Tenants["ta"]["api"]
	l.MaxConcurrent = 2
	good.Tenants["ta"]["api"] = l
	if err := g.ReplaceConfig(context.Background(), good); err != nil {
		t.Fatalf("valid config must install: %v", err)
	}
	if got := g.CurrentConfig().Version; got != 3 {
		t.Fatalf("expected v3, got %d", got)
	}
}

// TestDistributedWeightedSharing 验证跨实例的加权份额：
// B(权重3) 先借满容量 4；A(权重1) 通过另一实例加入排队；
// B 释放名额后 A 获得其份额 1，B 保留 3。
func TestDistributedWeightedSharing(t *testing.T) {
	cfg := distributedConfig()
	cfg.Tenants = map[string]map[string]TenantLimit{
		"ta": {"api": {RatePerSec: 1e6, Burst: 1e6, MaxConcurrent: 0, Weight: 1}},
		"tb": {"api": {RatePerSec: 1e6, Burst: 1e6, MaxConcurrent: 0, Weight: 3}},
	}
	baseURL, store, shutdown := startCoordinator(t, cfg)
	defer shutdown()
	gA := newRemoteGateway(t, cfg, baseURL, "inst-a")
	gB := newRemoteGateway(t, cfg, baseURL, "inst-b")

	var bLeases []*Lease
	for i := 0; i < 4; i++ {
		l, err := gB.Acquire(context.Background(), "tb", "api")
		if err != nil {
			t.Fatalf("B borrow %d: %v", i, err)
		}
		bLeases = append(bLeases, l)
	}
	if got := store.active["tb\x00api"]; got != 4 {
		t.Fatalf("B should hold 4 borrowed slots, got %d", got)
	}

	aGot := make(chan *Lease, 1)
	aErr := make(chan error, 1)
	go func() {
		l, err := gA.Acquire(context.Background(), "ta", "api")
		if err != nil {
			aErr <- err
			return
		}
		aGot <- l
	}()

	// B 释放一个，触发 A 收回份额
	time.Sleep(100 * time.Millisecond)
	bLeases[0].Release()

	select {
	case l := <-aGot:
		defer l.Release()
		if got := store.active["ta\x00api"]; got != 1 {
			t.Fatalf("A must hold its weighted share 1, got %d", got)
		}
		if got := store.active["tb\x00api"]; got != 3 {
			t.Fatalf("B must retain weighted share 3, got %d", got)
		}
	case err := <-aErr:
		t.Fatalf("A should acquire its share cross-instance: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("A timed out waiting for weighted share")
	}
	for _, l := range bLeases[1:] {
		l.Release()
	}
}
