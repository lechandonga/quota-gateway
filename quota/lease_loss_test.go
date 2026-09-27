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

// outageStore 模拟“连不上后端”：down=true 时 TryAcquire/Renew 失败，
// Release 始终放行（释放路径尽力而为，失败另有 TTL 兜底）。
type outageStore struct {
	inner *store.MemoryStore
	down  atomic.Bool
}

func (s *outageStore) TryAcquire(ctx context.Context, req store.AcquireRequest) (store.Result, error) {
	if s.down.Load() {
		return store.Result{}, errors.New("backend unreachable")
	}
	return s.inner.TryAcquire(ctx, req)
}

func (s *outageStore) Release(ctx context.Context, keys []string, token string) error {
	return s.inner.Release(ctx, keys, token)
}

func (s *outageStore) Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) ([]string, error) {
	if s.down.Load() {
		return nil, errors.New("backend unreachable")
	}
	return s.inner.Renew(ctx, keys, tokens, ttl)
}

func (s *outageStore) Ping(ctx context.Context) error { return s.inner.Ping(ctx) }
func (s *outageStore) Close() error                   { return nil }

func newLossManager(t *testing.T, st store.Store, rec *metrics.Recorder, ttl, renew time.Duration) *Manager {
	t.Helper()
	m, err := NewManager(Options{
		Store: st, Config: longConnConfig(), Metrics: rec,
		ConcurrencyTTL: ttl, RenewInterval: renew,
		ReapInterval: 20 * time.Millisecond, QueueTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func leaseRestoredTotal(rec *metrics.Recorder) int64 {
	return rec.Snapshot()["lease_restored_total"].(int64)
}

// 回归：长请求在途期间后端重启（记录全部丢失）——请求仍在处理、实例存活，
// 名额必须被续租重建并一直被占着，且丢失事件可从指标观测。
func TestLease_BackendRecordLostMidRequest_Restored(t *testing.T) {
	rec := metrics.New()
	ms := store.NewMemoryStore()
	m := newLossManager(t, ms, rec, 150*time.Millisecond, 40*time.Millisecond)
	ctx := context.Background()

	rel, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got := ms.Inflight("cc:t1:/long"); got != 1 {
		t.Fatalf("inflight=%d want 1", got)
	}
	before := leaseRestoredTotal(rec)

	// 后端重启：名额记录全部丢失（请求仍在途、持有实例存活）。
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if got := ms.Inflight("cc:t1:/long"); got != 0 {
		t.Fatalf("record should be gone after backend restart, inflight=%d", got)
	}

	// 续租必须按原样重建名额（不得提前归还在途请求的名额）。
	deadline := time.Now().Add(2 * time.Second)
	for ms.Inflight("cc:t1:/long") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("lost slot not restored by renew while request still in-flight")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 丢失/重建事件可观测。
	if got := leaseRestoredTotal(rec) - before; got < 1 {
		t.Fatalf("lease_restored_total delta=%d want >=1", got)
	}
	// 重建后新请求仍被正确拒绝（名额一直被原请求占着）。
	if _, err := m.Acquire(ctx, "t1", "/long"); err == nil {
		t.Fatal("newcomer must be rejected after slot restored")
	} else if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited, got %v", err)
	}
	// 请求正常结束：名额正常回收，不泄漏。
	rel()
	deadline = time.Now().Add(2 * time.Second)
	for ms.Inflight("cc:t1:/long") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("slot leaked after release")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 回归：长请求在途期间连不上后端、后端记录因 TTL 过期而丢失——恢复连接后
// 续租必须重建名额；整个过程中本地租约不被提前释放，事件可观测。
func TestLease_BackendOutageExpiryThenRestore(t *testing.T) {
	rec := metrics.New()
	os := &outageStore{inner: store.NewMemoryStore()}
	m := newLossManager(t, os, rec, 150*time.Millisecond, 40*time.Millisecond)
	ctx := context.Background()

	rel, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	before := leaseRestoredTotal(rec)

	// 连不上后端超过 TTL：续租持续失败，后端记录过期（名额丢失状态）。
	os.down.Store(true)
	time.Sleep(300 * time.Millisecond)
	if got := os.inner.Inflight("cc:t1:/long"); got != 0 {
		t.Fatalf("record should have expired during outage, inflight=%d", got)
	}
	// 本地租约必须仍在（请求在途、实例存活，不得提前归还）。
	if n := m.leases.len(); n != 1 {
		t.Fatalf("local lease must survive backend outage, leases=%d", n)
	}

	// 后端恢复：下一个续租周期必须重建名额。
	os.down.Store(false)
	deadline := time.Now().Add(2 * time.Second)
	for os.inner.Inflight("cc:t1:/long") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("expired slot not restored after backend recovery")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := leaseRestoredTotal(rec) - before; got < 1 {
		t.Fatalf("lease_restored_total delta=%d want >=1", got)
	}
	// 新请求仍被拒：运行中的数量不会因记录丢失而超过上限。
	if _, err := m.Acquire(ctx, "t1", "/long"); err == nil {
		t.Fatal("newcomer must be rejected after slot restored")
	}
	rel()
	deadline = time.Now().Add(2 * time.Second)
	for os.inner.Inflight("cc:t1:/long") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("slot leaked after release")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := m.leases.len(); n != 0 {
		t.Fatalf("lease registry leaked: %d entries", n)
	}
}
