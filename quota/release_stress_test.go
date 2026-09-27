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

// 释放路径并发交错稳定性：正常结束、客户端中断、重复释放、乱序释放
// 以任意顺序并发触发，进程不得崩溃，结束后不得泄漏名额。
// 在 -race 下反复运行（go test -race -count=N）。
func TestRelease_ConcurrentInterleavingsNoCrashNoLeak(t *testing.T) {
	cfg := &Config{
		Version:             "1.0",
		Degrade:             DegradeDeny,
		GlobalMaxConcurrent: 256,
		Rules: []Rule{
			{Tenant: "t1", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: 128, Weight: 1},
		},
	}
	m, ms := newLeaseManager(t, cfg, 300*time.Millisecond, 80*time.Millisecond)

	const goroutines = 32
	const iterations = 100
	var acquired atomic.Int64
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
					continue // 容量竞争被拒：正常路径
				}
				acquired.Add(1)
				switch rng.Intn(6) {
				case 0: // 正常结束
					rel()
					cancel()
				case 1: // 重复释放
					rel()
					rel()
					cancel()
				case 2: // 先断连再释放
					cancel()
					rel()
				case 3: // 先释放再断连
					rel()
					cancel()
				case 4: // 仅断连：交给后台回收
					cancel()
				case 5: // 并发触发：断连与释放同时发生
					go rel()
					cancel()
				default: // 不可达；仅为保证所有路径都取消 ctx
					rel()
					cancel()
				}
			}
		}(int64(g))
	}
	wg.Wait()

	// 所有“仅断连”的在途必须由后台循环有界回收，最终零泄漏。
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ms.Inflight("cc:t1:/work") == 0 && ms.Inflight("sc:global") == 0 && m.leases.len() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leak: conc=%d shared=%d leases=%d (acquired=%d)",
				ms.Inflight("cc:t1:/work"), ms.Inflight("sc:global"), m.leases.len(), acquired.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// flakyReleaseStore 包装 MemoryStore，按概率让 Release/Renew 失败，
// 模拟后端抖动；TryAcquire 始终正常（不触发降级）。
type flakyReleaseStore struct {
	inner     *store.MemoryStore
	failEvery atomic.Int64 // 每 N 次 Release/Renew 失败一次；0=不失败
	calls     atomic.Int64
}

func (f *flakyReleaseStore) maybeFail() error {
	if n := f.failEvery.Load(); n > 0 && f.calls.Add(1)%n == 0 {
		return errors.New("backend flaky")
	}
	return nil
}

func (f *flakyReleaseStore) TryAcquire(ctx context.Context, req store.AcquireRequest) (store.Result, error) {
	return f.inner.TryAcquire(ctx, req)
}

func (f *flakyReleaseStore) Release(ctx context.Context, keys []string, token string) error {
	if err := f.maybeFail(); err != nil {
		return err
	}
	return f.inner.Release(ctx, keys, token)
}

func (f *flakyReleaseStore) Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) ([]string, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	return f.inner.Renew(ctx, keys, tokens, ttl)
}

func (f *flakyReleaseStore) Ping(ctx context.Context) error { return f.inner.Ping(ctx) }
func (f *flakyReleaseStore) Close() error                   { return nil }

// 后端抖动下释放/续租失败不得导致崩溃；失败遗留的占位由 TTL 有界兜底回收。
func TestRelease_BackendFlakyNoCrashTTLBackstop(t *testing.T) {
	cfg := &Config{
		Version: "1.0",
		Degrade: DegradeDeny,
		Rules: []Rule{
			{Tenant: "t1", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: 64, Weight: 1},
		},
	}
	inner := store.NewMemoryStore()
	fs := &flakyReleaseStore{inner: inner}
	fs.failEvery.Store(3) // 1/3 的 Release/Renew 失败
	ttl := 200 * time.Millisecond
	m, err := NewManager(Options{
		Store: fs, Config: cfg,
		ConcurrencyTTL: ttl, RenewInterval: 50 * time.Millisecond, ReapInterval: 20 * time.Millisecond,
		QueueTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				rel, err := m.Acquire(context.Background(), "t1", "/work")
				if err != nil {
					continue
				}
				rel()
			}
		}()
	}
	wg.Wait()

	// 释放失败遗留的占位：TTL 到期后惰性回收，最终零泄漏。
	deadline := time.Now().Add(5 * time.Second)
	for inner.Inflight("cc:t1:/work") != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("orphaned slots not reclaimed by TTL: inflight=%d", inner.Inflight("cc:t1:/work"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
