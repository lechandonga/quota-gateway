package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryStore_RateBurstAndRecovery(t *testing.T) {
	ms := NewMemoryStore()
	now := time.Unix(1000, 0)
	clock := now
	ms.SetNow(func() time.Time { return clock })

	acq := func() Result {
		r, err := ms.TryAcquire(context.Background(), AcquireRequest{
			RateKey: "r", ConcKey: "c", Rate: 10, Burst: 5, Token: "t", TTL: time.Second,
		})
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		return r
	}
	// 初始满桶：突发 5。
	for i := 0; i < 5; i++ {
		if r := acq(); r.Status != StatusAcquired {
			t.Fatalf("req %d: status=%d want acquired", i, r.Status)
		}
	}
	r := acq()
	if r.Status != StatusRateLimited || r.RetryAfter <= 0 {
		t.Fatalf("expected rate limited with retry hint, got %+v", r)
	}
	// 速率 10/s：100ms 后恢复 1 个令牌。
	clock = clock.Add(100 * time.Millisecond)
	if r := acq(); r.Status != StatusAcquired {
		t.Fatalf("after refill: status=%d want acquired", r.Status)
	}
	// 紧接着仍应为受限。
	if r := acq(); r.Status != StatusRateLimited {
		t.Fatalf("status=%d want rate limited", r.Status)
	}
	// 1 秒后应恢复到桶满（封顶，不超过 burst）。
	clock = clock.Add(time.Second)
	for i := 0; i < 5; i++ {
		if r := acq(); r.Status != StatusAcquired {
			t.Fatalf("recovered req %d: status=%d", i, r.Status)
		}
	}
}

func TestMemoryStore_ConcurrencyLimitAndRelease(t *testing.T) {
	ms := NewMemoryStore()
	for i := 0; i < 3; i++ {
		r, err := ms.TryAcquire(context.Background(), AcquireRequest{
			RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 3, Token: tok(i), TTL: time.Second,
		})
		if err != nil || r.Status != StatusAcquired {
			t.Fatalf("acquire %d: %+v %v", i, r, err)
		}
	}
	r, err := ms.TryAcquire(context.Background(), AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 3, Token: "t4", TTL: time.Second})
	if err != nil || r.Status != StatusConcurrencyLimited {
		t.Fatalf("expected concurrency limited, got %+v %v", r, err)
	}
	// 释放一个后立即可以再进。
	if err := ms.Release(context.Background(), []string{"c"}, tok(1)); err != nil {
		t.Fatal(err)
	}
	if r, _ := ms.TryAcquire(context.Background(), AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 3, Token: "t5", TTL: time.Second}); r.Status != StatusAcquired {
		t.Fatalf("after release: %+v", r)
	}
	// 重复释放 / 释放不存在的 token：无副作用，不影响计数。
	for i := 0; i < 3; i++ {
		if err := ms.Release(context.Background(), []string{"c"}, tok(1)); err != nil {
			t.Fatal(err)
		}
	}
	if got := ms.inflight("c"); got != 3 {
		t.Fatalf("inflight=%d want 3 (idempotent release)", got)
	}
}

func TestMemoryStore_TTLReclaimsDeadHolders(t *testing.T) {
	ms := NewMemoryStore()
	now := time.Unix(2000, 0)
	clock := now
	ms.SetNow(func() time.Time { return clock })

	for i := 0; i < 2; i++ {
		r, _ := ms.TryAcquire(context.Background(), AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 2, Token: tok(i), TTL: 100 * time.Millisecond})
		if r.Status != StatusAcquired {
			t.Fatalf("acquire %d: %+v", i, r)
		}
	}
	if r, _ := ms.TryAcquire(context.Background(), AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 2, Token: "x", TTL: 100 * time.Millisecond}); r.Status != StatusConcurrencyLimited {
		t.Fatalf("expected concurrency limited: %+v", r)
	}
	// 模拟实例崩溃：不调用 Release，只等 TTL 过期。
	clock = clock.Add(200 * time.Millisecond)
	r, _ := ms.TryAcquire(context.Background(), AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 2, Token: "y", TTL: 100 * time.Millisecond})
	if r.Status != StatusAcquired {
		t.Fatalf("dead holders should be reclaimed by TTL, got %+v", r)
	}
	// 过期 token 的乱序释放必须无害。
	if err := ms.Release(context.Background(), []string{"c"}, tok(0)); err != nil {
		t.Fatal(err)
	}
	if got := ms.inflight("c"); got != 1 {
		t.Fatalf("inflight=%d want 1", got)
	}
}

func TestMemoryStore_ConcurrentAcquireNoAmplification(t *testing.T) {
	ms := NewMemoryStore()
	const goroutines, maxConc = 200, 10
	var wg sync.WaitGroup
	granted := make(chan string, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r, err := ms.TryAcquire(context.Background(), AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: maxConc, Token: tok(i)})
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if r.Status == StatusAcquired {
				granted <- r.Token
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(granted)
	n := 0
	for range granted {
		n++
	}
	if n != maxConc {
		t.Fatalf("granted=%d want exactly %d (no amplification)", n, maxConc)
	}
	if got := ms.inflight("c"); got != maxConc {
		t.Fatalf("inflight=%d want %d", got, maxConc)
	}
}

func tok(i int) string {
	return "token-" + string(rune('a'+i))
}

func TestMemoryStore_SharedCapacityIndependentOfTenantCap(t *testing.T) {
	ms := NewMemoryStore()
	req := func(tenant string) AcquireRequest {
		return AcquireRequest{
			RateKey:       "r:" + tenant,
			ConcKey:       "cc:" + tenant,
			SharedKey:     "shared",
			Rate:          0,
			Burst:         1,
			MaxConcurrent: 10, // 单租户上限很宽松
			MaxShared:     2,  // 共享池只有 2
			Token:         tenant,
			TTL:           time.Second,
		}
	}
	for _, tenant := range []string{"a", "b"} {
		if r, err := ms.TryAcquire(context.Background(), req(tenant)); err != nil || r.Status != StatusAcquired {
			t.Fatalf("acquire %s: %+v %v", tenant, r, err)
		}
	}
	if r, _ := ms.TryAcquire(context.Background(), req("c")); r.Status != StatusSharedLimited {
		t.Fatalf("shared pool full: got %+v", r)
	}
	// 释放 a 的占位（同时释放租户键与共享键）后，c 可进入。
	if err := ms.Release(context.Background(), []string{"cc:a", "shared"}, "a"); err != nil {
		t.Fatal(err)
	}
	if r, _ := ms.TryAcquire(context.Background(), req("c")); r.Status != StatusAcquired {
		t.Fatalf("after shared release: got %+v", r)
	}
}
