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

func (s *toggleStore) Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) error {
	return s.inner.Renew(ctx, keys, tokens, ttl)
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
