package store

import (
	"context"
	"testing"
	"time"
)

// 续租保活：长连接在途期间反复 Renew，占位不得在原 TTL 时点被回收。
func TestMemoryStore_RenewKeepsLongConnectionSlot(t *testing.T) {
	ms := NewMemoryStore()
	now := time.Unix(3000, 0)
	clock := now
	ms.SetNow(func() time.Time { return clock })
	ctx := context.Background()
	ttl := 100 * time.Millisecond

	r, _ := ms.TryAcquire(ctx, AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 1, Token: "a", TTL: ttl})
	if r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v", r)
	}
	// 请求持续 5 倍 TTL，期间按周期续租。
	for i := 0; i < 5; i++ {
		clock = clock.Add(80 * time.Millisecond)
		if _, err := ms.Renew(ctx, []string{"c"}, []string{"a"}, ttl); err != nil {
			t.Fatalf("renew %d: %v", i, err)
		}
		if got := ms.Inflight("c"); got != 1 {
			t.Fatalf("round %d: inflight=%d want 1 (slot lost while alive)", i, got)
		}
		r, _ := ms.TryAcquire(ctx, AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 1, Token: "b", TTL: ttl})
		if r.Status != StatusConcurrencyLimited {
			t.Fatalf("round %d: newcomer must be rejected, got %+v", i, r)
		}
	}
}

// 续租不得复活已释放或已过期的占位（否则名额被重复计入、额度放大）。
func TestMemoryStore_RenewDoesNotResurrect(t *testing.T) {
	ms := NewMemoryStore()
	now := time.Unix(4000, 0)
	clock := now
	ms.SetNow(func() time.Time { return clock })
	ctx := context.Background()
	ttl := 100 * time.Millisecond

	r, _ := ms.TryAcquire(ctx, AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 1, Token: "a", TTL: ttl})
	if r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v", r)
	}
	// 场景 1：已释放的 token 续租不得复活。
	if err := ms.Release(ctx, []string{"c"}, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Renew(ctx, []string{"c"}, []string{"a"}, ttl); err != nil {
		t.Fatal(err)
	}
	if got := ms.Inflight("c"); got != 0 {
		t.Fatalf("released token resurrected: inflight=%d", got)
	}
	// 场景 2：过期 token 续租不得复活。
	r, _ = ms.TryAcquire(ctx, AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 1, Token: "b", TTL: ttl})
	if r.Status != StatusAcquired {
		t.Fatalf("re-acquire: %+v", r)
	}
	clock = clock.Add(2 * ttl) // b 过期
	if _, err := ms.Renew(ctx, []string{"c"}, []string{"b"}, ttl); err != nil {
		t.Fatal(err)
	}
	if got := ms.Inflight("c"); got != 0 {
		t.Fatalf("expired token resurrected: inflight=%d", got)
	}
}

// Redis 路径：续租保活 + 不复活已释放占位（经 fakeredis 走真实脚本）。
func TestRedisStore_RenewSemantics(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	st := newRedisStore(t, f)
	ctx := context.Background()
	ttl := 120 * time.Millisecond
	req := AcquireRequest{
		RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
		MaxConcurrent: 1, Token: "a", TTL: ttl,
	}
	r, err := st.TryAcquire(ctx, req)
	if err != nil || r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v %v", r, err)
	}
	// 持续 4 倍 TTL，期间续租：名额必须一直被占用。
	for i := 0; i < 4; i++ {
		time.Sleep(80 * time.Millisecond)
		if _, err := st.Renew(ctx, []string{"cc"}, []string{"a"}, ttl); err != nil {
			t.Fatalf("renew %d: %v", i, err)
		}
		r2, _ := st.TryAcquire(ctx, AcquireRequest{
			RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
			MaxConcurrent: 1, Token: "b", TTL: ttl,
		})
		if r2.Status != StatusConcurrencyLimited {
			t.Fatalf("round %d: newcomer must be rejected, got %+v", i, r2)
		}
	}
	// 释放后续租不得复活。
	if err := st.Release(ctx, []string{"cc"}, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Renew(ctx, []string{"cc"}, []string{"a"}, ttl); err != nil {
		t.Fatal(err)
	}
	r3, _ := st.TryAcquire(ctx, AcquireRequest{
		RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
		MaxConcurrent: 1, Token: "c", TTL: ttl,
	})
	if r3.Status != StatusAcquired {
		t.Fatalf("slot must be free after release, got %+v", r3)
	}
}

// 多实例：A 续租持长连接，B 始终看到名额被占；A 崩溃（停止续租）后
// B 在有界时间内获得名额，且全程不超过上限。
func TestRedisStore_RenewMultiInstanceCrashRecovery(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	a := newRedisStore(t, f)
	b := newRedisStore(t, f)
	ctx := context.Background()
	ttl := 150 * time.Millisecond
	mkReq := func(token string) AcquireRequest {
		return AcquireRequest{
			RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
			MaxConcurrent: 1, Token: token, TTL: ttl,
		}
	}
	r, err := a.TryAcquire(ctx, mkReq("inst-a-1"))
	if err != nil || r.Status != StatusAcquired {
		t.Fatalf("A acquire: %+v %v", r, err)
	}
	// A 存活并续租 3 轮：B 不得进入。
	for i := 0; i < 3; i++ {
		time.Sleep(90 * time.Millisecond)
		if _, err := a.Renew(ctx, []string{"cc"}, []string{"inst-a-1"}, ttl); err != nil {
			t.Fatalf("A renew %d: %v", i, err)
		}
		rb, _ := b.TryAcquire(ctx, mkReq("inst-b-1"))
		if rb.Status != StatusConcurrencyLimited {
			t.Fatalf("round %d: B must be rejected while A alive, got %+v", i, rb)
		}
	}
	// A 崩溃：不再续租也不释放。B 必须在 TTL 量级内有界获得名额。
	deadline := time.Now().Add(3 * time.Second)
	for {
		rb, _ := b.TryAcquire(ctx, mkReq("inst-b-1"))
		if rb.Status == StatusAcquired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("B did not acquire within bounded time after A crashed")
		}
		time.Sleep(30 * time.Millisecond)
	}
}
