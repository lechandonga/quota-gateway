package quota

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/metrics"
	"github.com/lechandonga/quota-gateway/store"
)

// toggleStore 可切换后端可用性：fail=true 时 TryAcquire 报错（触发降级）。
type toggleStore struct {
	inner *store.MemoryStore
	fail  atomic.Bool
}

func (s *toggleStore) TryAcquire(ctx context.Context, req store.AcquireRequest) (store.Result, error) {
	if s.fail.Load() {
		return store.Result{}, errors.New("backend down")
	}
	return s.inner.TryAcquire(ctx, req)
}

func (s *toggleStore) Release(ctx context.Context, keys []string, token string) error {
	return s.inner.Release(ctx, keys, token)
}

func (s *toggleStore) Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) ([]string, error) {
	return s.inner.Renew(ctx, keys, tokens, ttl)
}

func (s *toggleStore) Reclaim(ctx context.Context, req store.ReclaimRequest) (store.Status, error) {
	return s.inner.Reclaim(ctx, req)
}

func (s *toggleStore) Ping(ctx context.Context) error {
	if s.fail.Load() {
		return errors.New("backend down")
	}
	return s.inner.Ping(ctx)
}

func (s *toggleStore) Close() error { return nil }

func degradeConfig() *Config {
	return &Config{
		Version: "1.0",
		Degrade: DegradeAllow,
		Rules: []Rule{
			{Tenant: "tdeg", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
		},
	}
}

func newDegradeManager(t *testing.T, cfg *Config) (*Manager, *toggleStore) {
	t.Helper()
	ts := &toggleStore{inner: store.NewMemoryStore()}
	m, err := NewManager(Options{Store: ts, Config: cfg, QueueTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, ts
}

// 降级放行必须遵守租户并发硬上限：本地计数、本地回收、可观测。
func TestDegradeAllow_LocalHardCapEnforced(t *testing.T) {
	rec := metrics.New()
	rec.ResetForTest()
	m, ts := newDegradeManager(t, degradeConfig())
	m.metrics = rec
	ts.fail.Store(true)
	ctx := context.Background()

	before := rec.Snapshot()["degraded_inflight_by_key"].(map[string]int64)["tdeg|/work"]

	rel1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("degraded acquire 1: %v", err)
	}
	rel2, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("degraded acquire 2: %v", err)
	}
	// 第 3 个：达到本地硬上限，必须按并发超限拒绝（可解释）。
	_, err = m.Acquire(ctx, "tdeg", "/work")
	if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited at local cap, got %v", err)
	}
	if got := m.degradedLocal("tdeg", "/work"); got != 2 {
		t.Fatalf("degradedLocal=%d want 2", got)
	}
	after := rec.Snapshot()["degraded_inflight_by_key"].(map[string]int64)["tdeg|/work"]
	if after-before != 2 {
		t.Fatalf("degraded inflight gauge delta=%d want 2", after-before)
	}
	// 降级放行不得写入后端。
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 0 {
		t.Fatalf("degraded holds must not touch backend, inflight=%d", got)
	}

	// 本地回收后名额可复用。
	rel1()
	if got := m.degradedLocal("tdeg", "/work"); got != 1 {
		t.Fatalf("degradedLocal=%d want 1 after release", got)
	}
	rel3, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("degraded acquire after local release: %v", err)
	}
	rel2()
	rel3()
	if got := m.degradedLocal("tdeg", "/work"); got != 0 {
		t.Fatalf("degradedLocal=%d want 0 after all released", got)
	}
}

// 依赖恢复后重新纳入统一计账：不得放大额度，也不得把降级期间的在途
// 重复计数；本地在途随结束自然归零后恢复全额。
func TestDegradeAllow_RecoveryNoAmplificationNoDoubleCount(t *testing.T) {
	m, ts := newDegradeManager(t, degradeConfig())
	ctx := context.Background()

	// 降级期：本地放行 2 个（达到硬上限）。
	ts.fail.Store(true)
	l1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("degraded acquire 1: %v", err)
	}
	l2, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("degraded acquire 2: %v", err)
	}

	// 后端恢复：本地仍有 2 个在途，新请求不得放大额度（2+0 已达上限）。
	ts.fail.Store(false)
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
		t.Fatal("must not amplify quota while degraded in-flight holds the cap")
	} else if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited, got %v", err)
	}

	// 释放 1 个本地在途：新请求可进入，但占用后端额度（1 本地 + 1 后端 = 2）。
	l1()
	b1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("acquire after one local drained: %v", err)
	}
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 1 {
		t.Fatalf("backend inflight=%d want 1", got)
	}
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
		t.Fatal("local(1)+backend(1)=2 reached cap, must reject")
	}

	// 全部结束：本地与后端都归零，不重复计数、不泄漏。
	l2()
	b1()
	if got := m.degradedLocal("tdeg", "/work"); got != 0 {
		t.Fatalf("degradedLocal=%d want 0", got)
	}
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 0 {
		t.Fatalf("backend inflight=%d want 0", got)
	}

	// 恢复全额：可同时占用 2 个后端名额。
	r1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("post-recovery acquire 1: %v", err)
	}
	r2, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("post-recovery acquire 2: %v", err)
	}
	r1()
	r2()
}

// 降级期间的客户端断连：本地兜底名额同样被后台回收，不泄漏。
func TestDegradeAllow_DisconnectReclaimed(t *testing.T) {
	cfg := degradeConfig()
	ts := &toggleStore{inner: store.NewMemoryStore()}
	ts.fail.Store(true)
	m, err := NewManager(Options{
		Store: ts, Config: cfg,
		ReapInterval: 20 * time.Millisecond, QueueTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err != nil {
		t.Fatalf("degraded acquire: %v", err)
	}
	cancel() // 断连且不主动释放
	deadline := time.Now().Add(2 * time.Second)
	for m.degradedLocal("tdeg", "/work") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("degraded local slot not reclaimed after disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 核心回归：后端故障前已经在途的请求（持有后端名额）+ 放行期间新进来的
// 请求，在本实例上合计绝不能超过配置上限；在途随结束回收，恢复后计账正确。
func TestDegradeAllow_PreExistingInFlightPlusFailOpenNeverExceedsCap(t *testing.T) {
	rec := metrics.New()
	cfg := degradeConfig() // MaxConcurrent=2, DegradeAllow
	m, ts := newDegradeManager(t, cfg)
	m.metrics = rec
	ctx := context.Background()

	// 故障前：2 个请求已在途（占满后端名额）。
	pre1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("pre acquire 1: %v", err)
	}
	pre2, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("pre acquire 2: %v", err)
	}
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 2 {
		t.Fatalf("backend inflight=%d want 2 before outage", got)
	}

	// 后端不可用，切到放行：本地统一在途已经是 2，任何新请求都必须被拒，
	// 不能出现“老在途 2 + 新放行 2 = 4（约 2 倍上限）”。
	ts.fail.Store(true)
	for i := 0; i < 10; i++ {
		if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
			t.Fatalf("fail-open request %d admitted over cap with pre-existing inflight", i)
		} else if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
			t.Fatalf("request %d want concurrency_limited, got %v", i, err)
		}
	}
	if got := m.localInflightTotal("tdeg", "/work"); got != 2 {
		t.Fatalf("local inflight=%d want 2 during outage", got)
	}
	if got := m.degradedLocal("tdeg", "/work"); got != 0 {
		t.Fatalf("degraded inflight=%d want 0 (nothing new admitted)", got)
	}

	// 一个老请求结束：本地在途降到 1，放行可进 1 个（合计仍为 2）。
	pre1()
	deg1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("one fail-open slot after drain: %v", err)
	}
	if got := m.localInflightTotal("tdeg", "/work"); got != 2 {
		t.Fatalf("local inflight=%d want 2 (1 backend + 1 degraded)", got)
	}
	if got := m.degradedLocal("tdeg", "/work"); got != 1 {
		t.Fatalf("degraded inflight=%d want 1", got)
	}
	// 仍然不能再放。
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
		t.Fatal("cap must still hold with 1 backend + 1 degraded inflight")
	}
	// 放行期间的在途可观测。
	if got := rec.Snapshot()["degraded_inflight_by_key"].(map[string]int64)["tdeg|/work"]; got != 1 {
		t.Fatalf("degraded inflight metric=%d want 1", got)
	}

	// 后端恢复：剩余的 1 个降级在途从正常计账中扣除，新请求不得放大；
	// 也不得把降级在途重复算进后端。
	ts.fail.Store(false)
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
		t.Fatal("recovery must not amplify while 1 degraded slot still counted")
	}
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 1 {
		t.Fatalf("backend inflight=%d want 1 (only pre2 remains; degraded never wrote backend)", got)
	}

	// 全部结束：本地与后端归零、租约表清空，不泄漏、不重复归还。
	deg1()
	pre2()
	waitForDegradeDrain(m, "tdeg", "/work")
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 0 {
		t.Fatalf("backend inflight=%d want 0 after all done", got)
	}
	if m.leases.len() != 0 {
		t.Fatalf("lease registry leaked: %d", m.leases.len())
	}
}

// 规则未配置 MaxConcurrent 时，降级放行使用 DegradeMaxConcurrent 兜底，
// 且同样把故障前在途算入本地统一上限。
func TestDegradeAllow_DefaultCapIncludesPreExisting(t *testing.T) {
	cfg := &Config{
		Version: "1.0",
		Degrade: DegradeAllow,
		Rules: []Rule{
			{Tenant: "tn", API: "/work", Rate: 0, Burst: 1, Weight: 1}, // 无上限
		},
	}
	ts := &toggleStore{inner: store.NewMemoryStore()}
	m, err := NewManager(Options{
		Store: ts, Config: cfg,
		DegradeMaxConcurrent: 3, QueueTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	ctx := context.Background()

	var rels []ReleaseFunc
	// 故障前在途 2 个（无 MaxConcurrent，后端不限；本地仍按兜底上限计数）。
	ts.fail.Store(true) // 直接在故障态获取，走放行，模拟“本地已有在途”。
	for i := 0; i < 2; i++ {
		r, err := m.Acquire(ctx, "tn", "/work")
		if err != nil {
			t.Fatalf("degraded acquire %d: %v", i, err)
		}
		rels = append(rels, r)
	}
	// 第 3 个可放，第 4 个必须被兜底上限拒绝。
	r3, err := m.Acquire(ctx, "tn", "/work")
	if err != nil {
		t.Fatalf("3rd degraded acquire: %v", err)
	}
	rels = append(rels, r3)
	if _, err := m.Acquire(ctx, "tn", "/work"); err == nil {
		t.Fatal("4th request must hit DegradeMaxConcurrent")
	}
	for _, r := range rels {
		r()
	}
}

func waitForDegradeDrain(m *Manager, tenant, api string) {
	deadline := time.Now().Add(2 * time.Second)
	for m.localInflightTotal(tenant, api) != 0 || m.degradedLocal(tenant, api) != 0 {
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
