package quota

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

// longConnConfig 单租户、仅并发硬上限 1，便于确定性观察名额占用。
func longConnConfig() *Config {
	return &Config{
		Version: "1.0",
		Degrade: DegradeDeny,
		Rules: []Rule{
			{Tenant: "t1", API: "/long", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
		},
	}
}

func newLeaseManager(t *testing.T, cfg *Config, ttl, renew time.Duration) (*Manager, *store.MemoryStore) {
	t.Helper()
	ms := store.NewMemoryStore()
	m, err := NewManager(Options{
		Store:          ms,
		Config:         cfg,
		ConcurrencyTTL: ttl,
		RenewInterval:  renew,
		ReapInterval:   20 * time.Millisecond,
		QueueTimeout:   200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, ms
}

// 长连接在途期间（远超 TTL）名额必须一直被占用：第二个请求不得进入。
func TestLease_LongConnectionKeepsSlotBeyondTTL(t *testing.T) {
	m, ms := newLeaseManager(t, longConnConfig(), 150*time.Millisecond, 40*time.Millisecond)
	ctx := context.Background()

	rel, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer rel()

	// 持续运行时间远超 TTL：续租必须保住名额。
	time.Sleep(500 * time.Millisecond)
	if got := ms.Inflight("cc:t1:/long"); got != 1 {
		t.Fatalf("inflight=%d want 1 (slot lost while request alive)", got)
	}
	if _, err := m.Acquire(ctx, "t1", "/long"); err == nil {
		t.Fatal("second acquire must be rejected while long connection holds the slot")
	} else if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited, got %v", err)
	}
}

// 持有方崩溃（不再续租、也不释放）后，名额必须在 TTL 量级内有界回收，
// 且回收前不得放大额度。
func TestLease_CrashRecoversSlotWithinBoundedTime(t *testing.T) {
	ttl := 150 * time.Millisecond
	// RenewInterval 极大：模拟持有实例崩溃（不再续租）。
	m, ms := newLeaseManager(t, longConnConfig(), ttl, time.Hour)
	ctx := context.Background()

	rel, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = rel // 故意不释放：模拟崩溃遗留的占位。

	// TTL 到期前：名额仍被占用，不得放大。
	if _, err := m.Acquire(ctx, "t1", "/long"); err == nil {
		t.Fatal("slot must still be held before TTL expiry")
	}
	// 有界回收：TTL 之后新请求可以进入。
	deadline := time.Now().Add(3 * time.Second)
	for {
		rel2, err := m.Acquire(ctx, "t1", "/long")
		if err == nil {
			rel2()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("orphaned slot not reclaimed within bounded time (inflight=%d)", ms.Inflight("cc:t1:/long"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// 已释放的名额不得被续租“复活”：释放后等待多个续租周期，在途保持 0。
func TestLease_ReleasedSlotIsNotResurrected(t *testing.T) {
	m, ms := newLeaseManager(t, longConnConfig(), 150*time.Millisecond, 40*time.Millisecond)
	ctx := context.Background()

	rel, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	rel()
	rel() // 重复释放必须无害

	time.Sleep(400 * time.Millisecond) // 覆盖多个续租周期
	if got := ms.Inflight("cc:t1:/long"); got != 0 {
		t.Fatalf("inflight=%d want 0 (released slot resurrected)", got)
	}
	if n := m.leases.len(); n != 0 {
		t.Fatalf("lease registry leaked: %d entries", n)
	}
}

// 调用方 ctx 取消（客户端断连）但业务未主动释放时，后台循环必须有界回收。
func TestLease_CanceledContextReapedInBackground(t *testing.T) {
	m, ms := newLeaseManager(t, longConnConfig(), 150*time.Millisecond, 40*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	_, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// 故意不调用 Release，仅取消 ctx（断连且 handler 未返回的场景）。
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for ms.Inflight("cc:t1:/long") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("canceled lease not reaped in background")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := m.leases.len(); n != 0 {
		t.Fatalf("lease registry leaked: %d entries", n)
	}
}
