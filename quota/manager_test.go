package quota

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

func newTestManager(t *testing.T, cfg *Config) (*Manager, *store.MemoryStore) {
	t.Helper()
	ms := store.NewMemoryStore()
	m, err := NewManager(Options{Store: ms, Config: cfg, QueueTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, ms
}

// baseConfig: 全局共享池 2，租户硬上限各自 2。
func baseConfig(degrade DegradePolicy) *Config {
	return &Config{
		Version:             "1.0",
		Degrade:             degrade,
		GlobalMaxConcurrent: 2,
		Rules: []Rule{
			{Tenant: "t1", API: "/fast", Rate: 5, Burst: 5, MaxConcurrent: 2, Weight: 1},
			{Tenant: "t1", API: "/slow", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
			{Tenant: "t2", API: "/slow", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
			{Tenant: "t3", API: "/slow", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
		},
	}
}

func TestAcquire_AllowThenRateLimitWithRetryHint(t *testing.T) {
	m, _ := newTestManager(t, baseConfig(DegradeDeny))
	ctx := context.Background()
	// burst=5，逐个获取并立即释放（释放不归还速率令牌，仅清在途）。
	for i := 0; i < 5; i++ {
		rel, err := m.Acquire(ctx, "t1", "/fast")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		rel()
	}
	_, err := m.Acquire(ctx, "t1", "/fast")
	qe, ok := AsError(err)
	if !ok || qe.Code != ErrCodeRateLimited {
		t.Fatalf("want rate_limited, got %v (ok=%v)", err, ok)
	}
	if qe.RetryAfterSeconds() <= 0 {
		t.Fatalf("expected positive retry-after, got %s", qe.RetryAfter)
	}
}

func TestAcquire_RateRecoveryServesAgain(t *testing.T) {
	ms := store.NewMemoryStore()
	cfg := baseConfig(DegradeDeny)
	m, _ := NewManager(Options{Store: ms, Config: cfg, QueueTimeout: time.Second})
	clock := time.Unix(1000, 0)
	ms.SetNow(func() time.Time { return clock })
	// 速率 5/s 恰好对应 cfg 规则。
	for i := 0; i < 5; i++ {
		rel, err := m.Acquire(context.Background(), "t1", "/fast")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		rel()
	}
	if _, err := m.Acquire(context.Background(), "t1", "/fast"); err == nil {
		t.Fatal("expected rate limited")
	}
	// 1 秒后令牌恢复。
	clock = clock.Add(time.Second)
	if _, err := m.Acquire(context.Background(), "t1", "/fast"); err != nil {
		t.Fatalf("after recovery should pass: %v", err)
	}
}

func TestAcquire_UnconfiguredTenantPassesThrough(t *testing.T) {
	m, _ := newTestManager(t, baseConfig(DegradeDeny))
	rel, err := m.Acquire(context.Background(), "ghost", "/fast")
	if err != nil {
		t.Fatalf("unconfigured tenant should pass, got %v", err)
	}
	rel() // no-op 也必须安全
	rel()
}

func TestAcquire_TenantHardLimitRejectsImmediately(t *testing.T) {
	// 不设全局池：仅租户硬上限生效，应立即拒绝而不排队。
	cfg := &Config{Version: "1.0", Rules: []Rule{
		{Tenant: "t1", API: "/x", Burst: 1, MaxConcurrent: 1, Weight: 1},
	}}
	m, _ := newTestManager(t, cfg)
	r1, err := m.Acquire(context.Background(), "t1", "/x")
	if err != nil {
		t.Fatal(err)
	}
	defer r1()
	start := time.Now()
	_, err = m.Acquire(context.Background(), "t1", "/x")
	elapsed := time.Since(start)
	qe, ok := AsError(err)
	if !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited, got %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("hard limit must reject immediately, took %s", elapsed)
	}
}

func TestAcquire_LongConnectionReleasePaths(t *testing.T) {
	m, _ := newTestManager(t, baseConfig(DegradeDeny))
	ctx := context.Background()

	occupy := func(tenant string) ReleaseFunc {
		rel, err := m.Acquire(ctx, tenant, "/slow")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		return rel
	}
	r1 := occupy("t1")
	r2 := occupy("t2")

	// 共享池占满：不持有 token 的请求会公平排队，在有界等待后被拒绝。
	_, err := m.Acquire(ctx, "t3", "/slow")
	if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited after queue wait, got %v", err)
	}

	// 正常结束释放后可立即再进（闲置容量归还）。
	r1()
	rel3 := occupy("t3")

	// 重复释放与乱序释放：不泄漏、不提前归还。
	r1()
	r2()
	r2()
	rel3()
	if err := m.schedWaitZero(); err != nil {
		t.Fatal(err)
	}

	// 现在应能重新占满共享池 2 个槽（说明之前全部正确归还）。
	a := occupy("t1")
	b := occupy("t2")
	defer a()
	defer b()
}

func TestAcquire_ClientCancelWhileQueuedDoesNotLeak(t *testing.T) {
	m, _ := newTestManager(t, baseConfig(DegradeDeny))
	r1, _ := m.Acquire(context.Background(), "t1", "/slow")
	r2, _ := m.Acquire(context.Background(), "t2", "/slow")
	defer r1()
	defer r2()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.Acquire(ctx, "t3", "/slow")
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // 确保 t3 已入公平队列
	cancel()                          // 客户端中断
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled acquire did not return")
	}

	// 释放一个槽，验证取消的排队者没有“幽灵占位”，新请求可立即进入。
	r1()
	rel, err := m.Acquire(context.Background(), "t1", "/slow")
	if err != nil {
		t.Fatalf("canceled waiter must not leak a slot: %v", err)
	}
	rel()
}

func TestAcquire_QueuedRequestProceedsWhenCapacityFreed(t *testing.T) {
	m, _ := newTestManager(t, baseConfig(DegradeDeny))
	r1, _ := m.Acquire(context.Background(), "t1", "/slow")
	r2, _ := m.Acquire(context.Background(), "t2", "/slow")
	defer r2()

	got := make(chan ReleaseFunc, 1)
	go func() {
		rel, err := m.Acquire(context.Background(), "t3", "/slow")
		if err != nil {
			t.Errorf("queued acquire should succeed after release: %v", err)
			got <- nil
			return
		}
		got <- rel
	}()
	time.Sleep(50 * time.Millisecond)
	r1() // 归还容量
	select {
	case rel := <-got:
		if rel == nil {
			t.Fatal("acquire failed")
		}
		rel()
	case <-time.After(800 * time.Millisecond):
		t.Fatal("queued request was not admitted after capacity freed")
	}
}

func TestAcquire_DegradeDenyAndAllow(t *testing.T) {
	failing := &failingStore{err: errors.New("redis gone")}

	m, err := NewManager(Options{Store: failing, Config: baseConfig(DegradeDeny)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Acquire(context.Background(), "t1", "/fast")
	if qe, ok := AsError(err); !ok || qe.Code != ErrCodeDegradedDeny {
		t.Fatalf("degrade deny: got %v", err)
	}

	failing.err = nil // 恢复后立刻正常（不残留降级状态）
	rel, err := m.Acquire(context.Background(), "t1", "/fast")
	if err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	rel()

	// fail-open
	failing.err = errors.New("redis down again")
	m2, _ := NewManager(Options{Store: failing, Config: baseConfig(DegradeAllow)})
	rel, err = m2.Acquire(context.Background(), "t1", "/fast")
	if err != nil {
		t.Fatalf("degrade allow should pass, got %v", err)
	}
	rel() // 放行请求不持有后端占位，释放必须无害
}

func TestUpdateConfig_InvalidKeepsOldConfig(t *testing.T) {
	m, _ := newTestManager(t, baseConfig(DegradeDeny))
	if v := m.ConfigVersion(); v != "1.0" {
		t.Fatalf("version=%q", v)
	}
	bad := &Config{Version: "1.0", Rules: []Rule{{Tenant: "x", Rate: 1}}} // burst 缺失
	if err := m.UpdateConfig(bad); err == nil {
		t.Fatal("expected invalid config rejected")
	}
	if v := m.ConfigVersion(); v != "1.0" {
		t.Fatalf("old config must remain, got version=%q", v)
	}

	good := &Config{Version: "1.4", GlobalMaxConcurrent: 4,
		Rules: []Rule{{Tenant: "z", Rate: 1, Burst: 1}}}
	if err := m.UpdateConfig(good); err != nil {
		t.Fatalf("valid update: %v", err)
	}
	if v := m.ConfigVersion(); v != "1.4" {
		t.Fatalf("version=%q want 1.4", v)
	}

	incompat := &Config{Version: "2.0"}
	if err := m.UpdateConfig(incompat); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("want incompatible error, got %v", err)
	}
	if v := m.ConfigVersion(); v != "1.4" {
		t.Fatalf("version=%q want still 1.4", v)
	}
}

// TestAcquire_SharedPoolDrainsFairly：超过共享容量的并发请求（不同租户，
// 不触及单租户硬上限）最终全部被服务，且任意时刻在途不超过共享池大小。
func TestAcquire_SharedPoolDrainsFairly(t *testing.T) {
	cfg := &Config{Version: "1.0", GlobalMaxConcurrent: 2, Rules: nil}
	for i := 0; i < 20; i++ {
		cfg.Rules = append(cfg.Rules, Rule{
			Tenant: "tt" + itoa(i), API: "/slow", Burst: 1,
			MaxConcurrent: 2, Weight: 1,
		})
	}
	m, _ := newTestManager(t, cfg)
	const n = 20
	var granted atomic.Int64
	var maxSeen atomic.Int64
	var inflight atomic.Int64
	var wg sync.WaitGroup
	gate := make(chan struct{})
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		tenant := "tt" + itoa(i)
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			<-gate
			rel, err := m.Acquire(context.Background(), tenant, "/slow")
			if err != nil {
				errs <- err
				return
			}
			cur := inflight.Add(1)
			for {
				old := maxSeen.Load()
				if cur <= old || maxSeen.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inflight.Add(-1)
			granted.Add(1)
			rel()
		}(tenant)
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := granted.Load(); got != n {
		t.Fatalf("granted=%d want %d (fair queue should drain)", got, n)
	}
	if got := maxSeen.Load(); got > 2 {
		t.Fatalf("peak inflight=%d exceeds shared pool 2", got)
	}
}

// TestAcquire_WeightsDetermineOrder 验证权重影响虚拟完成时间顺序：
// 在同一共享池内，两个权重悬殊的租户竞争时，高权重者应先被放行。
func TestAcquire_WeightsDetermineOrder(t *testing.T) {
	cfg := &Config{Version: "1.0", GlobalMaxConcurrent: 1, Rules: []Rule{
		{Tenant: "hi", API: "/x", Burst: 1, MaxConcurrent: 10, Weight: 100},
		{Tenant: "lo", API: "/x", Burst: 1, MaxConcurrent: 10, Weight: 1},
	}}
	ms := store.NewMemoryStore()
	m, err := NewManager(Options{Store: ms, Config: cfg, QueueTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// 先占满唯一共享槽（由 lo 持有）。
	holder, _ := m.Acquire(context.Background(), "lo", "/x")

	var wg sync.WaitGroup
	order := make(chan string, 2)
	start := make(chan struct{})
	for _, tenant := range []string{"lo", "hi"} {
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			<-start
			rel, err := m.Acquire(context.Background(), tenant, "/x")
			if err != nil {
				t.Errorf("acquire %s: %v", tenant, err)
				return
			}
			order <- tenant
			rel()
		}(tenant)
	}
	close(start)
	time.Sleep(50 * time.Millisecond) // 两者均已排队
	holder()                          // 释放唯一共享槽

	first := <-order
	second := <-order
	wg.Wait()
	if first != "hi" {
		t.Fatalf("first admitted=%q want hi (weight 100 before weight 1)", first)
	}
	if second != "lo" {
		t.Fatalf("second admitted=%q want lo", second)
	}
}

// schedWaitZero 检查慢接口各租户调度状态归零。
func (m *Manager) schedWaitZero() error {
	m.sched.mu.Lock()
	defer m.sched.mu.Unlock()
	for tenant, fs := range m.sched.flows {
		if strings.HasPrefix(tenant, "t") && (fs.inflight != 0 || fs.waiting != 0) {
			return errors.New("scheduler state not drained for " + tenant)
		}
	}
	return nil
}

// failingStore 是可控故障的 Store。
type failingStore struct {
	err error
}

func (f *failingStore) TryAcquire(context.Context, store.AcquireRequest) (store.Result, error) {
	if f.err != nil {
		return store.Result{}, f.err
	}
	return store.Result{Status: store.StatusAcquired}, nil
}

func (f *failingStore) Release(context.Context, []string, string) error { return f.err }
func (f *failingStore) Renew(context.Context, []string, string, time.Duration) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return true, nil
}
func (f *failingStore) RenewAll(_ context.Context, items []store.RenewItem) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return nil, nil
}
func (f *failingStore) Ping(context.Context) error { return f.err }
func (f *failingStore) Close() error               { return nil }
