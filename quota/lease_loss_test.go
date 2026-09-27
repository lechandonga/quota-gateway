package quota

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/metrics"
	"github.com/lechandonga/quota-gateway/store"
)

// recordLossStore 包装 MemoryStore，可在指定 token 上模拟“后端记录中途
// 丢失”：删除其在租户键（及共享键）上的占位，如同后端重启/键被清理。
type recordLossStore struct {
	inner *store.MemoryStore
	mu    sync.Mutex
}

func newRecordLossStore() *recordLossStore {
	return &recordLossStore{inner: store.NewMemoryStore()}
}

func (s *recordLossStore) TryAcquire(ctx context.Context, req store.AcquireRequest) (store.Result, error) {
	return s.inner.TryAcquire(ctx, req)
}

func (s *recordLossStore) Release(ctx context.Context, keys []string, token string) error {
	return s.inner.Release(ctx, keys, token)
}

func (s *recordLossStore) Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) ([]string, error) {
	return s.inner.Renew(ctx, keys, tokens, ttl)
}

func (s *recordLossStore) Reclaim(ctx context.Context, req store.ReclaimRequest) (store.Status, error) {
	return s.inner.Reclaim(ctx, req)
}

func (s *recordLossStore) Ping(ctx context.Context) error { return s.inner.Ping(ctx) }
func (s *recordLossStore) Close() error                   { return nil }

// loseToken 删除指定 token 在所有键上的后端记录（名额中途丢失）。
func (s *recordLossStore) loseToken(token string) {
	s.inner.LoseHolders(token)
}

func lossConfig() *Config {
	return &Config{
		Version: "1.0",
		Degrade: DegradeDeny,
		Rules: []Rule{
			{Tenant: "tlong", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
		},
	}
}

func newLossManager(t *testing.T, cfg *Config) (*Manager, *recordLossStore) {
	t.Helper()
	rs := newRecordLossStore()
	m, err := NewManager(Options{
		Store: rs, Config: cfg,
		ConcurrencyTTL: 300 * time.Millisecond,
		RenewInterval:  40 * time.Millisecond,
		ReapInterval:   20 * time.Millisecond,
		QueueTimeout:   200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, rs
}

func waitFor(t *testing.T, deadline time.Duration, cond func() bool, msg string) {
	t.Helper()
	end := time.Now().Add(deadline)
	for !cond() {
		if time.Now().After(end) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 场景：长请求在途期间后端记录中途丢失——名额必须被自动补占，
// 新请求始终进不来（不超额），且丢失/补占可从指标看出。
func TestLease_BackendRecordLossIsReclaimed(t *testing.T) {
	cfg := &Config{
		Version: "1.0",
		Degrade: DegradeDeny,
		Rules: []Rule{
			{Tenant: "tlong", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
		},
	}
	rec := metrics.New()
	rec.ResetForTest()
	m, rs := newLossManager(t, cfg)
	m.metrics = rec
	ctx := context.Background()

	rel, err := m.Acquire(ctx, "tlong", "/work")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer rel()

	// 找到该租约的 token 并模拟后端记录中途丢失。
	entries := m.leases.snapshot()
	if len(entries) != 1 {
		t.Fatalf("leases=%d want 1", len(entries))
	}
	tok := entries[0].token
	rs.loseToken(tok)
	if got := rs.inner.Inflight("cc:tlong:/work"); got != 0 {
		t.Fatalf("after loss inflight=%d want 0", got)
	}

	// 记录丢失后、补占完成前：若没有补占，新请求会立即占进空位导致超额。
	// 等待后台循环发现丢失并完成补占。
	waitFor(t, 2*time.Second, func() bool {
		return rs.inner.Inflight("cc:tlong:/work") == 1
	}, "lost slot not reclaimed while request still in flight")

	// 补占后新请求不得进入（名额仍属于在途的长请求，不超额）。
	if _, err := m.Acquire(ctx, "tlong", "/work"); err == nil {
		t.Fatal("new request must not enter after reclaim (would exceed cap)")
	} else if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited, got %v", err)
	}

	// 指标可观测：丢失与补占各 1 次。
	snap := rec.Snapshot()
	if got := snap["lease_lost_total"]; got != int64(1) {
		t.Fatalf("lease_lost_total=%v want 1", got)
	}
	if got := snap["lease_reclaimed_total"]; got != int64(1) {
		t.Fatalf("lease_reclaimed_total=%v want 1", got)
	}
}

// 场景：补占完成后长请求正常结束，名额被正常释放、可复用，不泄漏。
func TestLease_ReclaimedLeaseReleasesNormally(t *testing.T) {
	m, rs := newLossManager(t, lossConfig())
	ctx := context.Background()

	rel, err := m.Acquire(ctx, "tlong", "/work")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	tok := m.leases.snapshot()[0].token
	rs.loseToken(tok)
	waitFor(t, 2*time.Second, func() bool {
		return rs.inner.Inflight("cc:tlong:/work") == 1
	}, "lost slot not reclaimed")

	// 长请求结束：补占回来的名额必须被正常删除（幂等，一次）。
	rel()
	waitFor(t, time.Second, func() bool {
		return rs.inner.Inflight("cc:tlong:/work") == 0 && m.leases.len() == 0
	}, "reclaimed slot not released after request end")

	// 名额可复用。
	r2, err := m.Acquire(ctx, "tlong", "/work")
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	r2()
}

// 场景：补占时后端上限恰好被其他实例占满——Reclaim 必须等待空位
// （不放大额度），期间本实例保留补占预留；空位出现后下个续租周期自动
// 补占成功。这里用同一后端上的另一个 Manager 模拟“其他实例”。
func TestLease_ReclaimWaitsWhenCapFullThenRecovers(t *testing.T) {
	cfg := lossConfig() // 上限 2
	rs := newRecordLossStore()
	newM := func(ri time.Duration) *Manager {
		mm, err := NewManager(Options{
			Store: rs, Config: cfg,
			ConcurrencyTTL: 300 * time.Millisecond,
			RenewInterval:  ri, ReapInterval: 20 * time.Millisecond,
			QueueTimeout: 200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewManager: %v", err)
		}
		t.Cleanup(func() { _ = mm.Close() })
		return mm
	}
	m := newM(40 * time.Millisecond) // 本实例（长请求持有方）
	other := newM(time.Hour)         // 模拟“其他实例”，不续租
	ctx := context.Background()

	// 本实例的长请求占 1 个名额。
	aRel, err := m.Acquire(ctx, "tlong", "/work")
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	aTok := m.leases.snapshot()[0].token

	// a 的后端记录中途丢失。其他实例立即占满因丢失空出的名额：
	// other 占 2 个（丢失后后端占用为 0，other 直接占满 2）。
	rs.loseToken(aTok)
	o1, err := other.Acquire(ctx, "tlong", "/work")
	if err != nil {
		t.Fatalf("other acquire 1: %v", err)
	}
	o2, err := other.Acquire(ctx, "tlong", "/work")
	if err != nil {
		t.Fatalf("other acquire 2: %v", err)
	}

	// 本实例后台补占：后端被 other 占满 2，Reclaim 受阻，保留预留等待。
	waitFor(t, 2*time.Second, func() bool {
		return m.pendingForKey("cc:tlong:/work") >= 1
	}, "pending reclaim reservation not registered")

	// 其他实例释放 1 个：空位出现，本实例下周期补占成功。
	o1()
	waitFor(t, 2*time.Second, func() bool {
		return rs.inner.Inflight("cc:tlong:/work") == 2 &&
			m.pendingForKey("cc:tlong:/work") == 0
	}, "reclaim did not complete after slot freed")

	// a 结束：补占回来的名额被正常释放，不泄漏、不重复归还。
	aRel()
	o2()
	waitFor(t, time.Second, func() bool {
		return rs.inner.Inflight("cc:tlong:/work") == 0 && m.leases.len() == 0
	}, "slots leaked after reclaim + release")
}

// 仅受全局共享池约束（规则 MaxConcurrent=0）的在途名额丢失后，
// 仍必须被自动补占（只持有共享键的租约）。
func TestLease_SharedOnlyRecordLossIsReclaimed(t *testing.T) {
	cfg := &Config{
		Version: "1.0", Degrade: DegradeDeny, GlobalMaxConcurrent: 1,
		Rules: []Rule{
			{Tenant: "ts", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: 0, Weight: 1},
		},
	}
	m, rs := newLossManager(t, cfg)
	ctx := context.Background()

	rel, err := m.Acquire(ctx, "ts", "/work")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer rel()
	if got := rs.inner.Inflight("sc:global"); got != 1 {
		t.Fatalf("shared inflight=%d want 1", got)
	}
	tok := m.leases.snapshot()[0].token
	rs.loseToken(tok) // 只丢共享键记录
	waitFor(t, 2*time.Second, func() bool {
		return rs.inner.Inflight("sc:global") == 1
	}, "shared-only lost slot not reclaimed")
	if _, err := m.Acquire(ctx, "ts", "/work"); err == nil {
		t.Fatal("new request must be blocked by reclaimed shared slot")
	}
}
