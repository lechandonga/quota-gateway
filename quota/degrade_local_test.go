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

// 可控故障存储：可切换后端可用/不可用，恢复后走真实 MemoryStore。
type switchableStore struct {
	mu       sync.Mutex
	down     bool
	inner    *store.MemoryStore
	downHits atomic.Int64
}

func (s *switchableStore) setDown(v bool) {
	s.mu.Lock()
	s.down = v
	s.mu.Unlock()
}

func (s *switchableStore) isDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.down
}

func (s *switchableStore) TryAcquire(ctx context.Context, req store.AcquireRequest) (store.Result, error) {
	if s.isDown() {
		s.downHits.Add(1)
		return store.Result{}, errors.New("backend down")
	}
	return s.inner.TryAcquire(ctx, req)
}

func (s *switchableStore) Release(ctx context.Context, keys []string, token string) error {
	if s.isDown() {
		return errors.New("backend down")
	}
	return s.inner.Release(ctx, keys, token)
}

func (s *switchableStore) Renew(ctx context.Context, keys []string, token string, ttl time.Duration) (bool, error) {
	if s.isDown() {
		return false, errors.New("backend down")
	}
	return s.inner.Renew(ctx, keys, token, ttl)
}

func (s *switchableStore) RenewAll(ctx context.Context, items []store.RenewItem) ([]string, error) {
	if s.isDown() {
		return nil, errors.New("backend down")
	}
	return s.inner.RenewAll(ctx, items)
}

func (s *switchableStore) Ping(context.Context) error {
	if s.isDown() {
		return errors.New("backend down")
	}
	return nil
}

func (s *switchableStore) Close() error { return s.inner.Close() }

// 降级放行期间仍遵守租户并发硬上限：超额请求被稳定拒绝（429 同构），
// 本地在途可观测，释放后名额正常回收。
func TestDegradeAllow_LocalCapEnforcedAndReclaimed(t *testing.T) {
	ms := store.NewMemoryStore()
	sw := &switchableStore{inner: ms}
	sw.setDown(true)
	cfg := &Config{Version: "1.0", Degrade: DegradeAllow, Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
	}}
	m, err := NewManager(Options{Store: sw, Config: cfg, ConcurrencyTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	r1, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatalf("degraded allow 1: %v", err)
	}
	r2, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatalf("degraded allow 2: %v", err)
	}
	if got := m.gate.inflight("t1\x00/c"); got != 2 {
		t.Fatalf("local inflight=%d want 2 (observable)", got)
	}
	// 第三个请求必须撞本地硬上限，不得无限堆积。
	_, err = m.Acquire(ctx, "t1", "/c")
	if qe, ok := AsError(err); !ok || qe.Code != ErrCodeDegradedLocalLimited {
		t.Fatalf("want degraded_local_concurrency_limited, got %v", err)
	}
	// 后端全程不可用：后端无占位（不产生幽灵远程名额）。
	if got := ms.Inflight("cc:t1:/c"); got != 0 {
		t.Fatalf("backend inflight during outage=%d want 0", got)
	}
	// 释放一个本地名额后可再进。
	r1()
	if got := m.gate.inflight("t1\x00/c"); got != 1 {
		t.Fatalf("local inflight=%d want 1 after release", got)
	}
	r3, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatalf("after local release should admit: %v", err)
	}
	r3()
	r2()
	if got := m.gate.inflight("t1\x00/c"); got != 0 {
		t.Fatalf("local inflight=%d want 0 after all done", got)
	}
}

// 恢复过渡期：降级期间的在途请求继续占用本地名额直到结束；恢复后的新请求
// 由后端计账。任意时刻本实例该租户在途 <= 上限，且同一请求不被两边重复计数。
func TestDegradeAllow_RecoveryNoAmplificationNoDoubleCount(t *testing.T) {
	ms := store.NewMemoryStore()
	sw := &switchableStore{inner: ms}
	sw.setDown(true)
	const capN int64 = 3
	cfg := &Config{Version: "1.0", Degrade: DegradeAllow, Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: capN, Weight: 1},
	}}
	m, err := NewManager(Options{Store: sw, Config: cfg, ConcurrencyTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	// 降级期间占满本地名额（d=capN）。
	rels := make([]ReleaseFunc, 0, capN)
	for i := int64(0); i < capN; i++ {
		rel, err := m.Acquire(ctx, "t1", "/c")
		if err != nil {
			t.Fatalf("degraded acquire %d: %v", i, err)
		}
		rels = append(rels, rel)
	}

	// 后端恢复：降级请求仍在运行。此时新请求走后端；本地闸门已满 capN，
	// 必须被拒——保证本机在途绝不超过 capN（本地旧占 + 后端新占不叠加超额）。
	sw.setDown(false)
	if _, err := m.Acquire(ctx, "t1", "/c"); err == nil {
		t.Fatal("new request during recovery with full local cap must be rejected")
	} else if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited during recovery, got %v", err)
	}

	// 一个降级请求结束：本地名额归还，后端名额立即可用。
	rels[0]()
	rel, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatalf("after degraded request finishes, backend slot must open: %v", err)
	}
	// 此时：本地旧占 2 + 后端新占 1 = capN，不放大。
	if total := m.gate.inflight("t1\x00/c"); total != capN {
		t.Fatalf("total local-gate inflight=%d want %d", total, capN)
	}
	if got := ms.Inflight("cc:t1:/c"); got != 1 {
		t.Fatalf("backend inflight=%d want 1 (not double-counted)", got)
	}
	rel()
	rels[1]()
	rels[2]()
	// 全部排空后：本地与后端都归零，后续请求正常以后端计账。
	if got := m.gate.inflight("t1\x00/c"); got != 0 {
		t.Fatalf("local inflight=%d want 0", got)
	}
	if got := ms.Inflight("cc:t1:/c"); got != 0 {
		t.Fatalf("backend inflight=%d want 0", got)
	}
	r2, err := m.Acquire(ctx, "t1", "/c")
	if err != nil {
		t.Fatalf("post-recovery acquire: %v", err)
	}
	r2()
}

// 降级 deny 不受本地闸门影响：恢复路径与既有语义一致（回归）。
func TestDegradeDeny_RecoveryUsesBackend(t *testing.T) {
	ms := store.NewMemoryStore()
	sw := &switchableStore{inner: ms}
	sw.setDown(true)
	cfg := &Config{Version: "1.0", Degrade: DegradeDeny, Rules: []Rule{
		{Tenant: "t1", API: "/c", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
	}}
	m, err := NewManager(Options{Store: sw, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Acquire(context.Background(), "t1", "/c"); err == nil {
		t.Fatal("degrade deny must reject")
	}
	sw.setDown(false)
	rel, err := m.Acquire(context.Background(), "t1", "/c")
	if err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	rel()
}
