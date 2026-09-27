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
		lost, err := ms.Renew(ctx, []string{"c"}, []string{"a"}, ttl)
		if err != nil {
			t.Fatalf("renew %d: %v", i, err)
		}
		if len(lost) != 0 {
			t.Fatalf("round %d: slot lost while alive: %v", i, lost)
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

// 后端记录中途丢失（重启/过期）时，续租必须按原样重建占位并以 lost
// 上报——只要持有方仍在续租（请求在途、实例存活），名额就一直被占着。
func TestMemoryStore_RenewRestoresLostSlots(t *testing.T) {
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
	// 场景 1：后端记录丢失（模拟后端重启清空），续租重建并上报 lost。
	if err := ms.Close(); err != nil { // Close 清空全部状态，模拟后端重启
		t.Fatal(err)
	}
	lost, err := ms.Renew(ctx, []string{"c"}, []string{"a"}, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 1 || lost[0] != "a" {
		t.Fatalf("lost=%v want [a]", lost)
	}
	if got := ms.Inflight("c"); got != 1 {
		t.Fatalf("lost slot not restored: inflight=%d", got)
	}
	// 重建后新请求仍被正确拒绝（名额一直被原持有者占着）。
	r, _ = ms.TryAcquire(ctx, AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 1, Token: "b", TTL: ttl})
	if r.Status != StatusConcurrencyLimited {
		t.Fatalf("newcomer must be rejected after restore, got %+v", r)
	}
	// 场景 2：记录过期（连不上后端超过 TTL）后续租同样重建并上报。
	clock = clock.Add(2 * ttl) // a 的记录过期
	lost, err = ms.Renew(ctx, []string{"c"}, []string{"a"}, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 1 || lost[0] != "a" {
		t.Fatalf("lost=%v want [a] after expiry", lost)
	}
	if got := ms.Inflight("c"); got != 1 {
		t.Fatalf("expired slot not restored: inflight=%d", got)
	}
	// 持有者真正结束后：Release 清掉占位，不再续租，名额不泄漏。
	if err := ms.Release(ctx, []string{"c"}, "a"); err != nil {
		t.Fatal(err)
	}
	if got := ms.Inflight("c"); got != 0 {
		t.Fatalf("released slot leaked: inflight=%d", got)
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
		lost, err := st.Renew(ctx, []string{"cc"}, []string{"a"}, ttl)
		if err != nil {
			t.Fatalf("renew %d: %v", i, err)
		}
		if len(lost) != 0 {
			t.Fatalf("round %d: slot lost while alive: %v", i, lost)
		}
		r2, _ := st.TryAcquire(ctx, AcquireRequest{
			RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
			MaxConcurrent: 1, Token: "b", TTL: ttl,
		})
		if r2.Status != StatusConcurrencyLimited {
			t.Fatalf("round %d: newcomer must be rejected, got %+v", i, r2)
		}
	}
	// 后端记录丢失（清空脚本缓存之外的全部数据，模拟重启）：续租重建并上报。
	f.flushAll()
	lost, err := st.Renew(ctx, []string{"cc"}, []string{"a"}, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 1 || lost[0] != "a" {
		t.Fatalf("lost=%v want [a]", lost)
	}
	r3, _ := st.TryAcquire(ctx, AcquireRequest{
		RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
		MaxConcurrent: 1, Token: "c", TTL: ttl,
	})
	if r3.Status != StatusConcurrencyLimited {
		t.Fatalf("newcomer must be rejected after restore, got %+v", r3)
	}
	// 持有者结束后正常释放，名额可回收复用。
	if err := st.Release(ctx, []string{"cc"}, "a"); err != nil {
		t.Fatal(err)
	}
	r4, _ := st.TryAcquire(ctx, AcquireRequest{
		RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
		MaxConcurrent: 1, Token: "d", TTL: ttl,
	})
	if r4.Status != StatusAcquired {
		t.Fatalf("slot must be free after release, got %+v", r4)
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
