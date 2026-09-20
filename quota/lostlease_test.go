package quota

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

// lostOnceStore 包装内存存储：触发“丢失”后续租返回 (false,nil)，
// 模拟实例失联超过 TTL、占位已被后端回收的场景。
type lostOnceStore struct {
	mu    sync.Mutex
	lost  bool
	inner *store.MemoryStore
}

func (s *lostOnceStore) TryAcquire(ctx context.Context, req store.AcquireRequest) (store.Result, error) {
	return s.inner.TryAcquire(ctx, req)
}
func (s *lostOnceStore) Release(ctx context.Context, keys []string, token string) error {
	return s.inner.Release(ctx, keys, token)
}
func (s *lostOnceStore) Renew(ctx context.Context, keys []string, token string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	lost := s.lost
	s.mu.Unlock()
	if lost {
		return false, nil
	}
	return s.inner.Renew(ctx, keys, token, ttl)
}

func (s *lostOnceStore) RenewAll(ctx context.Context, items []store.RenewItem) ([]string, error) {
	s.mu.Lock()
	lost := s.lost
	s.mu.Unlock()
	if lost {
		// 模拟后端已回收所有占位：全部凭证都报“丢失”。
		missing := make([]string, 0, len(items))
		for _, it := range items {
			missing = append(missing, it.Token)
		}
		return missing, nil
	}
	return s.inner.RenewAll(ctx, items)
}
func (s *lostOnceStore) Ping(context.Context) error { return nil }
func (s *lostOnceStore) Close() error               { return s.inner.Close() }

// 凭证在后端丢失后：本地仍继续占用名额直到请求结束，绝不因“丢约”而提前
// 归还导致本机多放行；请求结束后本地名额正常归还，系统自愈合。
func TestLeaseLost_LocalSlotHeldUntilRequestEnds(t *testing.T) {
	ms := store.NewMemoryStore()
	fc := &fakeClock{t: time.Unix(10000, 0)}
	ms.SetNow(fc.get)
	ls := &lostOnceStore{inner: ms}

	cfg := &Config{Version: "1.0", Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
	}}
	m, err := NewManager(Options{Store: ls, Config: cfg, ConcurrencyTTL: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	rel, err := m.Acquire(context.Background(), "t1", "/c")
	if err != nil {
		t.Fatal(err)
	}
	// 触发后端丢失，并推进时钟越过 TTL（后端占位可被回收）。
	ls.mu.Lock()
	ls.lost = true
	ls.mu.Unlock()
	fc.add(time.Second)
	time.Sleep(350 * time.Millisecond) // 等待续租器观察到“凭证丢失”

	// 本地闸门仍保守占用：即便后端名额已可被他人获得，本机也不多放行。
	if got := m.gate.inflight("t1\x00/c"); got != 1 {
		t.Fatalf("local inflight after lease lost=%d want 1 (must not early-return)", got)
	}
	if _, err := m.Acquire(context.Background(), "t1", "/c"); err == nil {
		t.Fatal("local gate must keep rejecting while the orphaned request still runs")
	}

	// 请求真正结束：本地名额归还；此后恢复正常。
	rel()
	if got := m.gate.inflight("t1\x00/c"); got != 0 {
		t.Fatalf("local inflight=%d want 0 after request ends", got)
	}
	r2, err := m.Acquire(context.Background(), "t1", "/c")
	if err != nil {
		t.Fatalf("after self-heal should admit: %v", err)
	}
	r2()
}
