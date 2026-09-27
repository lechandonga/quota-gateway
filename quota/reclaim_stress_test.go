package quota

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

// errFlaky 模拟后端抖动导致的释放/补占失败。
var errFlaky = errors.New("backend flaky")

// lossyStore 在 flakyReleaseStore 基础上，随机删除随机在途 token 的后端
// 记录（模拟后端抖动/重启导致名额中途丢失），并让 Reclaim 偶发失败。
type lossyStore struct {
	*recordLossStore
	flaky     atomic.Int64 // 每 N 次 Release/Reclaim 失败一次；0=不失败
	calls     atomic.Int64
	loseEvery atomic.Int64 // 每 N 次 TryAcquire 随机丢一个在途名额；0=不丢
	acqCalls  atomic.Int64
}

func (s *lossyStore) Release(ctx context.Context, keys []string, token string) error {
	if n := s.flaky.Load(); n > 0 && s.calls.Add(1)%n == 0 {
		return errFlaky
	}
	return s.recordLossStore.Release(ctx, keys, token)
}

func (s *lossyStore) Reclaim(ctx context.Context, req store.ReclaimRequest) (store.Status, error) {
	if n := s.flaky.Load(); n > 0 && s.calls.Add(1)%n == 0 {
		return 0, errFlaky
	}
	return s.recordLossStore.Reclaim(ctx, req)
}

func (s *lossyStore) maybeLose(m *Manager, tenant, api string) {
	n := s.loseEvery.Load()
	if n == 0 || s.acqCalls.Add(1)%n != 0 {
		return
	}
	es := m.leases.snapshot()
	backend := make([]*leaseEntry, 0, len(es))
	for _, e := range es {
		if e.tenant == tenant && e.api == api && len(e.keys) > 0 {
			backend = append(backend, e)
		}
	}
	if len(backend) == 0 {
		return
	}
	victim := backend[rand.Intn(len(backend))]
	victim.mu.Lock()
	lost := victim.lost
	victim.mu.Unlock()
	if !lost {
		s.loseToken(victim.token)
	}
}

// 高并发/高循环：正常结束、超时、客户端断连、重复释放、乱序释放与
// “名额中途丢失→补占”同时交错，进程不得崩溃、不得重复归还、不得泄漏。
// 建议：go test -race -count=N -run TestReclaim_ConcurrentInterleavings
func TestReclaim_ConcurrentInterleavings(t *testing.T) {
	cfg := &Config{
		Version:             "1.0",
		Degrade:             DegradeDeny,
		GlobalMaxConcurrent: 128,
		Rules: []Rule{
			{Tenant: "t1", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: 96, Weight: 1},
		},
	}
	rs := newRecordLossStore()
	ls := &lossyStore{recordLossStore: rs}
	ls.flaky.Store(5)     // 1/5 的释放/补占失败
	ls.loseEvery.Store(7) // 高频随机丢失在途名额
	m, err := NewManager(Options{
		Store: ls, Config: cfg,
		ConcurrencyTTL: time.Second, RenewInterval: 25 * time.Millisecond,
		ReapInterval: 10 * time.Millisecond, QueueTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	const goroutines = 24
	const iterations = 80
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < iterations; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				rel, err := m.Acquire(ctx, "t1", "/work")
				if err != nil {
					cancel()
					continue
				}
				ls.maybeLose(m, "t1", "/work")
				switch rng.Intn(6) {
				case 0:
					rel()
					cancel()
				case 1: // 重复释放
					rel()
					rel()
					cancel()
				case 2: // 先断连后释放
					cancel()
					rel()
				case 3: // 仅断连：后台回收
					cancel()
				case 4: // 丢失后立即释放（补占/释放竞态）
					ls.maybeLose(m, "t1", "/work")
					rel()
					cancel()
				default: // 释放与断连并发
					go rel()
					cancel()
				}
			}
		}(int64(g))
	}
	wg.Wait()

	// 停止后台随机丢失（避免收尾期间继续制造丢失），等待全部归零：
	// 租约表空、后端两个键空、补占预留空。
	ls.loseEvery.Store(0)
	ls.flaky.Store(0)
	deadline := time.Now().Add(8 * time.Second)
	for {
		cc, sc := rs.inner.Inflight("cc:t1:/work"), rs.inner.Inflight("sc:global")
		pending := m.pendingForKey("cc:t1:/work") + m.pendingForKey("sc:global")
		if cc == 0 && sc == 0 && m.leases.len() == 0 && pending == 0 &&
			m.localInflightTotal("t1", "/work") == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leak after storm: cc=%d sc=%d leases=%d pending=%d local=%d",
				cc, sc, m.leases.len(), pending, m.localInflightTotal("t1", "/work"))
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// 丢失补占与降级放行交错：后端在可用/不可用之间反复抖动，fail-open
// 本地硬上限与补占预留同时工作，全程不超额、结束后零泄漏。
func TestReclaim_DegradeFlappingNoExceedNoLeak(t *testing.T) {
	cfg := &Config{
		Version: "1.0",
		Degrade: DegradeAllow,
		Rules: []Rule{
			{Tenant: "tf", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: 8, Weight: 1},
		},
	}
	ts := &toggleStore{inner: store.NewMemoryStore()}
	m, err := NewManager(Options{
		Store: ts, Config: cfg,
		ConcurrencyTTL: 500 * time.Millisecond, RenewInterval: 20 * time.Millisecond,
		ReapInterval: 10 * time.Millisecond, QueueTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	const goroutines = 16
	const iterations = 60
	var peakLocal atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	go func() {
		rng := rand.New(rand.NewSource(99))
		for {
			select {
			case <-stop:
				return
			default:
				ts.fail.Store(rng.Intn(2) == 0)
				time.Sleep(time.Duration(rng.Intn(10)) * time.Millisecond)
			}
		}
	}()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < iterations; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				rel, err := m.Acquire(ctx, "tf", "/work")
				if err != nil {
					cancel()
					continue
				}
				cur := m.localInflightTotal("tf", "/work")
				for {
					old := peakLocal.Load()
					if cur <= old || peakLocal.CompareAndSwap(old, cur) {
						break
					}
				}
				// 极短在途，混合主动释放/断连回收。
				if rng.Intn(5) == 0 {
					cancel()
				} else {
					rel()
					cancel()
				}
			}
		}(int64(g))
	}
	wg.Wait()
	close(stop)
	ts.fail.Store(false)

	if got := peakLocal.Load(); got > 8 {
		t.Fatalf("peak local inflight=%d exceeds cap 8 during flapping degrade", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for m.localInflightTotal("tf", "/work") != 0 || m.leases.len() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("leak: local=%d leases=%d backend=%d",
				m.localInflightTotal("tf", "/work"), m.leases.len(),
				ts.inner.Inflight("cc:tf:/work"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := ts.inner.Inflight("cc:tf:/work"); got != 0 {
		t.Fatalf("backend inflight=%d want 0", got)
	}
}
