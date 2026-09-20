package quota

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

// fakeClock 是并发安全的测试时钟：续租 goroutine 与测试 goroutine 都会读。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) get() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) add(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

// 长时间运行的请求（持续远超租约 TTL）期间，续租必须持续有效：
// 同一租户的并发占用绝不能超过配置上限，请求结束后名额立即归还。
func TestAcquire_LongRequestLeaseRenewedNoEarlyReclaim(t *testing.T) {
	ms := store.NewMemoryStore()
	fc := &fakeClock{t: time.Unix(6000, 0)}
	ms.SetNow(fc.get)
	cfg := &Config{Version: "1.0", Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
	}}
	m, err := NewManager(Options{
		Store: ms, Config: cfg,
		ConcurrencyTTL: 300 * time.Millisecond, // 很短的租约
	})
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

	// 模拟长请求持续 3s（10 倍 TTL），期间推进虚拟时钟并等待真实续租周期。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fc.add(60 * time.Millisecond)
		time.Sleep(110 * time.Millisecond) // 等待后台续租（默认周期 TTL/3=100ms）
		// 名额仍被两个长请求占满：第三个请求必须被拒，不得超额放行。
		_, err := m.Acquire(ctx, "t1", "/c")
		if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
			t.Fatalf("during long request: want concurrency_limited, got %v (ok=%v)", err, ok)
		}
		if got := ms.Inflight("cc:t1:/c"); got != 2 {
			t.Fatalf("backend inflight=%d want 2 (lease must stay held)", got)
		}
	}

	// 一个长请求结束：名额立即归还，新请求立即进入（不依赖 TTL）。
	r1()
	r3, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatalf("slot must be available immediately after release: %v", err)
	}
	r3()
	r2()
	if got := ms.Inflight("cc:t1:/c"); got != 0 {
		t.Fatalf("inflight=%d want 0 after all releases", got)
	}
}

// 实例崩溃（停止续租且不 Release）后，遗留名额必须在一个 TTL 的有界
// 时间内被回收，后续请求不被永久阻塞。
func TestAcquire_DeadInstanceBoundedReclaim(t *testing.T) {
	ms := store.NewMemoryStore()
	fc := &fakeClock{t: time.Unix(7000, 0)}
	ms.SetNow(fc.get)
	cfg := &Config{Version: "1.0", Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
	}}
	ttl := 300 * time.Millisecond
	m, err := NewManager(Options{Store: ms, Config: cfg, ConcurrencyTTL: ttl})
	if err != nil {
		t.Fatal(err)
	}

	rel, err := m.Acquire(context.Background(), "t1", "/c")
	if err != nil {
		t.Fatal(err)
	}
	// 等待至少一次续租成功，证明崩溃前租约处于正常续期状态。
	time.Sleep(250 * time.Millisecond)
	if got := ms.Inflight("cc:t1:/c"); got != 1 {
		t.Fatalf("inflight before crash=%d want 1", got)
	}

	// 模拟实例被强杀：不调用 Release，直接 Close（续租 goroutine 停止）。
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	_ = rel

	// 新实例接管（同一后端）。
	fc.add(ttl + time.Millisecond) // 越过一个 TTL
	m2, err := NewManager(Options{Store: ms, Config: cfg, ConcurrencyTTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m2.Close() })
	r2, err := m2.Acquire(context.Background(), "t1", "/c")
	if err != nil {
		t.Fatalf("dead instance slot must be reclaimed within TTL, got %v", err)
	}
	r2()
}

// 续租期间后端持续不可用：不应 panic、不应重复释放；恢复后续租继续工作；
// 若失联超过 TTL 导致凭证丢失，要有可观测事件而不是悄悄放大或卡死。
type renewFlakyStore struct {
	mu        sync.Mutex
	inner     *store.MemoryStore
	failUntil time.Time
}

func (s *renewFlakyStore) TryAcquire(ctx context.Context, req store.AcquireRequest) (store.Result, error) {
	s.mu.Lock()
	failing := time.Now().Before(s.failUntil)
	s.mu.Unlock()
	if failing {
		return store.Result{}, errors.New("redis unavailable")
	}
	return s.inner.TryAcquire(ctx, req)
}

func (s *renewFlakyStore) Release(ctx context.Context, keys []string, token string) error {
	s.mu.Lock()
	failing := time.Now().Before(s.failUntil)
	s.mu.Unlock()
	if failing {
		return errors.New("redis unavailable")
	}
	return s.inner.Release(ctx, keys, token)
}

func (s *renewFlakyStore) Renew(ctx context.Context, keys []string, token string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	failing := time.Now().Before(s.failUntil)
	s.mu.Unlock()
	if failing {
		return false, errors.New("redis unavailable")
	}
	return s.inner.Renew(ctx, keys, token, ttl)
}

func (s *renewFlakyStore) RenewAll(ctx context.Context, items []store.RenewItem) ([]string, error) {
	s.mu.Lock()
	failing := time.Now().Before(s.failUntil)
	s.mu.Unlock()
	if failing {
		return nil, errors.New("redis unavailable")
	}
	return s.inner.RenewAll(ctx, items)
}

func (s *renewFlakyStore) Ping(context.Context) error { return nil }
func (s *renewFlakyStore) Close() error               { return s.inner.Close() }

func TestAcquire_RenewSurvivesBackendOutage(t *testing.T) {
	ms := store.NewMemoryStore() // 使用真实时钟：续租周期与 TTL 都是真实时间
	flaky := &renewFlakyStore{inner: ms}

	cfg := &Config{Version: "1.0", Degrade: DegradeDeny, Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
	}}
	m, err := NewManager(Options{
		Store: flaky, Config: cfg,
		ConcurrencyTTL: 600 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	rel, err := m.Acquire(context.Background(), "t1", "/c")
	if err != nil {
		t.Fatal(err)
	}
	// 后端短暂抖动（短于 TTL）：续租失败后下个周期自动恢复，名额不丢。
	flaky.mu.Lock()
	flaky.failUntil = time.Now().Add(250 * time.Millisecond)
	flaky.mu.Unlock()
	time.Sleep(700 * time.Millisecond)
	if got := ms.Inflight("cc:t1:/c"); got != 1 {
		t.Fatalf("inflight=%d want 1 after brief outage (< TTL)", got)
	}
	rel()
	if got := ms.Inflight("cc:t1:/c"); got != 0 {
		t.Fatalf("inflight=%d want 0 after release", got)
	}
}

// 大量并发长连接集中释放：释放不得 panic、不得泄漏，在途严格归零。
func TestRelease_HighConcurrencyInterleavingsNoLeak(t *testing.T) {
	ms := store.NewMemoryStore()
	cfg := &Config{Version: "1.0", Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 64, Weight: 1},
	}}
	m, err := NewManager(Options{Store: ms, Config: cfg, ConcurrencyTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	const loops, conc, capN = 8, 200, 64
	for loop := 0; loop < loops; loop++ {
		var wg sync.WaitGroup
		var violations atomic.Int64
		start := make(chan struct{})
		for i := 0; i < conc; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				rel, err := m.Acquire(context.Background(), "t1", "/c")
				if err != nil {
					// 达到上限是允许的（capN 上限、conc 并发）；被拒请求不持有名额。
					if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
						t.Errorf("unexpected acquire err: %v", err)
					}
					return
				}
				// 权威断言：准入成功的瞬间，本地闸门（与后端同构）在途绝不可能超额。
				if got := m.gate.inflight("t1\x00/c"); got > capN {
					violations.Add(1)
				}
				// 任意交错：立即释放（模拟 handler 结束与断连同时发生）。
				if i%3 == 0 {
					rel()
					rel() // 重复释放
				} else {
					time.Sleep(time.Duration(i%5) * time.Millisecond)
					rel()
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if got := violations.Load(); got != 0 {
			t.Fatalf("loop %d: %d observations exceeded cap %d", loop, got, capN)
		}
		if got := ms.Inflight("cc:t1:/c"); got != 0 {
			t.Fatalf("loop %d backend inflight=%d want 0 (leak)", loop, got)
		}
		if got := m.gate.inflight("t1\x00/c"); got != 0 {
			t.Fatalf("loop %d local inflight=%d want 0", loop, got)
		}
	}
}
