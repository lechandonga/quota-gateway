package store

import (
	"context"
	"testing"
	"time"
)

// 续期后凭证不应被 TTL 惰性回收：长请求的名额在续租期间始终占用。
func TestMemoryStore_RenewKeepsLongHolderAlive(t *testing.T) {
	ms := NewMemoryStore()
	now := time.Unix(3000, 0)
	clock := now
	ms.SetNow(func() time.Time { return clock })

	req := AcquireRequest{RateKey: "r", ConcKey: "c", SharedKey: "s",
		Burst: 1, MaxConcurrent: 1, MaxShared: 1, Token: "long", TTL: 100 * time.Millisecond}
	if r, err := ms.TryAcquire(context.Background(), req); err != nil || r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v %v", r, err)
	}
	// 请求持续 500ms（远超 TTL），每 30ms 续租一次。
	deadline := now.Add(500 * time.Millisecond)
	for clock.Before(deadline) {
		clock = clock.Add(30 * time.Millisecond)
		ok, err := ms.Renew(context.Background(), []string{"c", "s"}, "long", 100*time.Millisecond)
		if err != nil || !ok {
			t.Fatalf("renew at %s: ok=%v err=%v", clock.Sub(now), ok, err)
		}
		// 续租期间新请求不得挤入（租户键与共享键都仍被占用）。
		if r, _ := ms.TryAcquire(context.Background(), AcquireRequest{
			RateKey: "r2", ConcKey: "c", SharedKey: "s",
			Burst: 1, MaxConcurrent: 1, MaxShared: 1, Token: "other", TTL: time.Second,
		}); r.Status != StatusConcurrencyLimited {
			t.Fatalf("at %s: other request must not over-admit, got %+v", clock.Sub(now), r)
		}
	}
	if got := ms.inflight("c"); got != 1 {
		t.Fatalf("inflight tenant key=%d want 1", got)
	}
	if got := ms.inflight("s"); got != 1 {
		t.Fatalf("inflight shared key=%d want 1", got)
	}
	// 请求结束：释放后立即可以再进。
	if err := ms.Release(context.Background(), []string{"c", "s"}, "long"); err != nil {
		t.Fatal(err)
	}
	if r, _ := ms.TryAcquire(context.Background(), req); r.Status != StatusAcquired {
		t.Fatalf("after release: %+v", r)
	}
}

// 停止续租（模拟实例崩溃）后，占位必须在有界时间（TTL）内被回收。
func TestMemoryStore_RenewStopBoundedReclaim(t *testing.T) {
	ms := NewMemoryStore()
	now := time.Unix(4000, 0)
	clock := now
	ms.SetNow(func() time.Time { return clock })
	req := AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1,
		MaxConcurrent: 1, Token: "dead", TTL: 100 * time.Millisecond}
	if r, _ := ms.TryAcquire(context.Background(), req); r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v", r)
	}
	for i := 0; i < 3; i++ {
		clock = clock.Add(40 * time.Millisecond)
		ok, _ := ms.Renew(context.Background(), []string{"c"}, "dead", 100*time.Millisecond)
		if !ok {
			t.Fatal("renew should succeed while alive")
		}
	}
	// 实例在此崩溃：不再续租也不 Release。经过一个 TTL 后占位必须可回收。
	clock = clock.Add(101 * time.Millisecond)
	ok, err := ms.Renew(context.Background(), []string{"c"}, "dead", 100*time.Millisecond)
	if err != nil || ok {
		t.Fatalf("renew after crash: ok=%v err=%v want false", ok, err)
	}
	if r, _ := ms.TryAcquire(context.Background(), req); r.Status != StatusAcquired {
		t.Fatalf("dead holder must be reclaimed within TTL, got %+v", r)
	}
}

// 续期一个已被释放/已丢失的凭证：必须返回 false 且不得复活旧占位。
func TestMemoryStore_RenewMissingToken(t *testing.T) {
	ms := NewMemoryStore()
	req := AcquireRequest{RateKey: "r", ConcKey: "c", SharedKey: "s",
		Burst: 1, MaxConcurrent: 1, MaxShared: 1, Token: "t", TTL: time.Second}
	if r, _ := ms.TryAcquire(context.Background(), req); r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v", r)
	}
	if ok, _ := ms.Renew(context.Background(), []string{"c", "s"}, "ghost", time.Second); ok {
		t.Fatal("renew unknown token must return false")
	}
	if err := ms.Release(context.Background(), []string{"c", "s"}, "t"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := ms.Renew(context.Background(), []string{"c", "s"}, "t", time.Second); ok {
		t.Fatal("renew released token must return false, must not resurrect")
	}
	if got := ms.inflight("c"); got != 0 {
		t.Fatalf("inflight=%d want 0", got)
	}
	// 共享键上丢失（被 TTL 回收）但租户键仍在：整体续期必须失败且不修改。
	ms2 := NewMemoryStore()
	r2, _ := ms2.TryAcquire(context.Background(), AcquireRequest{
		RateKey: "r", ConcKey: "c", SharedKey: "s",
		Burst: 1, MaxConcurrent: 1, MaxShared: 1, Token: "t2", TTL: 10 * time.Millisecond})
	if r2.Status != StatusAcquired {
		t.Fatalf("acquire2: %+v", r2)
	}
	clock2 := time.Unix(5000, 0).Add(20 * time.Millisecond)
	ms2.SetNow(func() time.Time { return clock2 })
	// 先让共享键上的过期项在下一次访问时被清理；直接对已删状态续期返回 false。
	_ = ms2.Release(context.Background(), []string{"s"}, "t2")
	if ok, _ := ms2.Renew(context.Background(), []string{"c", "s"}, "t2", time.Second); ok {
		t.Fatal("partial loss must fail renewal atomically")
	}
}

// 假 Redis 路径：续期脚本往返与语义正确（多实例共享后端）。
func TestRedisStore_RenewKeepsHolderAlive(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	st := newRedisStore(t, f)
	req := AcquireRequest{RateKey: "rt", ConcKey: "cc", SharedKey: "shared",
		Rate: 0, Burst: 1, MaxConcurrent: 1, MaxShared: 1, Token: "tok", TTL: 30 * time.Millisecond}
	if r, err := st.TryAcquire(context.Background(), req); err != nil || r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v %v", r, err)
	}
	for i := 0; i < 5; i++ {
		time.Sleep(20 * time.Millisecond)
		ok, err := st.Renew(context.Background(), []string{"cc", "shared"}, "tok", 30*time.Millisecond)
		if err != nil || !ok {
			t.Fatalf("renew %d: ok=%v err=%v", i, ok, err)
		}
	}
	if ok, _ := st.Renew(context.Background(), []string{"cc", "shared"}, "ghost", time.Second); ok {
		t.Fatal("renew unknown token via redis must return false")
	}
	if err := st.Release(context.Background(), []string{"cc", "shared"}, "tok"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.Renew(context.Background(), []string{"cc", "shared"}, "tok", time.Second); ok {
		t.Fatal("renew after release must return false")
	}
}

// 多实例：实例 A 持有长连接并持续续租时，实例 B 在共享后端上不得超额放行。
func TestRedisStore_RenewMultiInstanceNoOverAdmit(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	a := newRedisStore(t, f)
	b := newRedisStore(t, f)
	req := AcquireRequest{RateKey: "rt", ConcKey: "cc",
		Rate: 0, Burst: 1, MaxConcurrent: 1, Token: "A-long", TTL: 30 * time.Millisecond}
	if r, err := a.TryAcquire(context.Background(), req); err != nil || r.Status != StatusAcquired {
		t.Fatalf("A acquire: %+v %v", r, err)
	}
	for i := 0; i < 4; i++ {
		time.Sleep(20 * time.Millisecond)
		ok, err := a.Renew(context.Background(), []string{"cc"}, "A-long", 30*time.Millisecond)
		if err != nil || !ok {
			t.Fatalf("A renew %d: ok=%v err=%v", i, ok, err)
		}
		// B 每次尝试都必须被租户硬上限挡住。
		if r, _ := b.TryAcquire(context.Background(), AcquireRequest{
			RateKey: "rt-b", ConcKey: "cc", Rate: 0, Burst: 1,
			MaxConcurrent: 1, Token: "B", TTL: time.Second,
		}); r.Status != StatusConcurrencyLimited {
			t.Fatalf("iteration %d: B must not over-admit, got %+v", i, r)
		}
	}
	// A 的长请求结束后 B 才可以进入。
	if err := a.Release(context.Background(), []string{"cc"}, "A-long"); err != nil {
		t.Fatal(err)
	}
	if r, _ := b.TryAcquire(context.Background(), AcquireRequest{
		RateKey: "rt-b", ConcKey: "cc", Rate: 0, Burst: 1,
		MaxConcurrent: 1, Token: "B2", TTL: time.Second,
	}); r.Status != StatusAcquired {
		t.Fatalf("B must acquire after A's long request ends, got %+v", r)
	}
}

// 批量续租（Redis 协议路径）：一次往返续多个凭证，部分丢失只报告丢失项，
// 存活项不被影响。
func TestRedisStore_RenewAllBatch(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	st := newRedisStore(t, f)
	for _, tok := range []string{"a", "b", "c"} {
		req := AcquireRequest{RateKey: "rt:" + tok, ConcKey: "cc:" + tok,
			Rate: 0, Burst: 1, MaxConcurrent: 1, Token: tok, TTL: 50 * time.Millisecond}
		if r, err := st.TryAcquire(context.Background(), req); err != nil || r.Status != StatusAcquired {
			t.Fatalf("acquire %s: %+v %v", tok, r, err)
		}
	}
	// 删掉 b 的占位（模拟已回收）。
	if err := st.Release(context.Background(), []string{"cc:b"}, "b"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond) // 让 a、c 过期，模拟“b 已删 + a/c 也过期”之外的纯缺失场景
	// 直接重新拿 a、c（短 TTL），然后只让 b 缺失：重拿全部后再删 b。
	for _, tok := range []string{"a", "c"} {
		req := AcquireRequest{RateKey: "rt2:" + tok, ConcKey: "cc:" + tok,
			Rate: 0, Burst: 1, MaxConcurrent: 1, Token: tok, TTL: time.Second}
		if r, _ := st.TryAcquire(context.Background(), req); r.Status != StatusAcquired {
			t.Fatalf("reacquire %s: %+v", tok, r)
		}
	}
	items := []RenewItem{
		{Token: "a", Keys: []string{"cc:a"}, TTL: time.Second},
		{Token: "b", Keys: []string{"cc:b"}, TTL: time.Second},
		{Token: "c", Keys: []string{"cc:c"}, TTL: time.Second},
	}
	missing, err := st.RenewAll(context.Background(), items)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != "b" {
		t.Fatalf("missing=%v want [b]", missing)
	}
}
