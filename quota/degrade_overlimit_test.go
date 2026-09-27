package quota

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

// 回归：后端短暂不可用切到放行后，故障前已在跑的请求必须计入本地上限——
// “故障前在途 + 放行新进”之和绝不超过配置上限，且上限本地可见、可回收。
func TestDegradeAllow_PreFailureInflightCountsTowardCap(t *testing.T) {
	m, ts := newDegradeManager(t, degradeConfig()) // tdeg /work MaxConcurrent=2, DegradeAllow
	ctx := context.Background()

	// 故障前：2 个请求经后端正常准入、仍在运行（达到配置上限）。
	b1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("pre-failure acquire 1: %v", err)
	}
	b2, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("pre-failure acquire 2: %v", err)
	}
	if backend, degraded := m.LocalInflight("tdeg", "/work"); backend != 2 || degraded != 0 {
		t.Fatalf("LocalInflight=(%d,%d) want (2,0)", backend, degraded)
	}

	// 后端短暂不可用：本实例在途已达上限，放行期间一个都不能多放
	// （修复前这里会再放 2 个，总数到 4 = 上限两倍）。
	ts.fail.Store(true)
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
		t.Fatal("degrade allow must not exceed the configured cap with pre-failure in-flight")
	} else if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited, got %v", err)
	}

	// 故障前的请求结束一个：放行可进一个，但总数仍不超上限。
	b1()
	d1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("degraded acquire after one drained: %v", err)
	}
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
		t.Fatal("local total already at cap, must reject")
	}
	if backend, degraded := m.LocalInflight("tdeg", "/work"); backend != 1 || degraded != 1 {
		t.Fatalf("LocalInflight=(%d,%d) want (1,1)", backend, degraded)
	}
	// 降级在途不写入后端：后端只有故障前 b2 的 1 个占位。
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 1 {
		t.Fatalf("backend inflight=%d want 1 (degraded holds must stay local)", got)
	}

	// 全部结束：本地两个计数都归零，不泄漏。
	b2()
	d1()
	if backend, degraded := m.LocalInflight("tdeg", "/work"); backend != 0 || degraded != 0 {
		t.Fatalf("LocalInflight=(%d,%d) want (0,0) after all released", backend, degraded)
	}
	if n := m.leases.len(); n != 0 {
		t.Fatalf("lease registry leaked: %d entries", n)
	}
}

// 回归：故障前在途与降级在途并存时后端恢复——重新按正常方式计账既不放大
// 额度，也不把放行期间的在途重复算一遍而误拒。
func TestDegradeAllow_RecoveryWithMixedInflight(t *testing.T) {
	m, ts := newDegradeManager(t, degradeConfig())
	ctx := context.Background()

	// 故障前 1 个在途；降级期间再放 1 个（本地总数 2 = 上限）。
	b1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("pre-failure acquire: %v", err)
	}
	ts.fail.Store(true)
	d1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("degraded acquire: %v", err)
	}

	// 后端恢复，两者都还在跑：新请求不得放大额度
	// （后端占位 1 + 本地降级在途 1 = 2 已达上限）。
	ts.fail.Store(false)
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
		t.Fatal("must not amplify quota right after recovery")
	} else if qe, ok := AsError(err); !ok || qe.Code != ErrCodeConcurrencyLimited {
		t.Fatalf("want concurrency_limited, got %v", err)
	}

	// 故障前的请求结束：可进 1 个（占后端名额），本地总数仍为 2。
	b1()
	n1, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("acquire after pre-failure drained: %v", err)
	}
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 1 {
		t.Fatalf("backend inflight=%d want 1", got)
	}
	if _, err := m.Acquire(ctx, "tdeg", "/work"); err == nil {
		t.Fatal("backend(1)+degraded(1)=2 reached cap, must reject")
	}

	// 降级在途结束：不重复计账，名额恢复全额，可再进 1 个。
	d1()
	n2, err := m.Acquire(ctx, "tdeg", "/work")
	if err != nil {
		t.Fatalf("acquire after degraded drained: %v", err)
	}
	n1()
	n2()
	if backend, degraded := m.LocalInflight("tdeg", "/work"); backend != 0 || degraded != 0 {
		t.Fatalf("LocalInflight=(%d,%d) want (0,0)", backend, degraded)
	}
	if got := ts.inner.Inflight("cc:tdeg:/work"); got != 0 {
		t.Fatalf("backend inflight=%d want 0 (leak)", got)
	}
}

// 回归压力：后端高频抖动（健康/降级来回切换）+ 高并发获取/释放/断连交错，
// 任意时刻本实例总在途不得超过配置上限，结束后零泄漏。
// 在 -race 下反复运行（go test -race -count=N ./quota/）。
func TestDegradeAllow_ConcurrentFlappingNeverExceedsCap(t *testing.T) {
	const capN = 4
	cfg := &Config{
		Version: "1.0",
		Degrade: DegradeAllow,
		Rules: []Rule{
			{Tenant: "tflap", API: "/work", Rate: 0, Burst: 1, MaxConcurrent: capN, Weight: 1},
		},
	}
	ts := &toggleStore{inner: store.NewMemoryStore()}
	m, err := NewManager(Options{
		Store: ts, Config: cfg,
		ConcurrencyTTL: 300 * time.Millisecond, RenewInterval: 50 * time.Millisecond,
		ReapInterval: 10 * time.Millisecond, QueueTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	// 后端抖动：每 1~3ms 翻转一次可用性。
	stop := make(chan struct{})
	var flapper sync.WaitGroup
	flapper.Add(1)
	go func() {
		defer flapper.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ts.fail.Store(true)
			time.Sleep(time.Millisecond)
			ts.fail.Store(false)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	var maxObserved atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				rel, err := m.Acquire(ctx, "tflap", "/work")
				if err != nil {
					cancel()
					continue
				}
				// 采样本实例总在途：任何时刻都不得超过配置上限。
				backend, degraded := m.LocalInflight("tflap", "/work")
				total := backend + degraded
				for { // 记录观测到的最大值
					old := maxObserved.Load()
					if total <= old || maxObserved.CompareAndSwap(old, total) {
						break
					}
				}
				switch (seed + i) % 4 {
				case 0: // 正常结束
					rel()
					cancel()
				case 1: // 重复释放
					rel()
					rel()
					cancel()
				case 2: // 断连后释放
					cancel()
					rel()
				case 3: // 仅断连：交后台回收
					cancel()
				}
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	flapper.Wait()

	if got := maxObserved.Load(); got > capN {
		t.Fatalf("local in-flight exceeded configured cap during flapping: max=%d cap=%d", got, capN)
	}

	// 全部结束/断连后：本地与后端都必须归零，不泄漏。
	deadline := time.Now().Add(5 * time.Second)
	for {
		backend, degraded := m.LocalInflight("tflap", "/work")
		if backend == 0 && degraded == 0 && m.leases.len() == 0 &&
			ts.inner.Inflight("cc:tflap:/work") == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leak after flapping: backend=%d degraded=%d leases=%d store=%d",
				backend, degraded, m.leases.len(), ts.inner.Inflight("cc:tflap:/work"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
