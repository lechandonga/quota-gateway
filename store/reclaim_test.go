package store

import (
	"context"
	"testing"
	"time"
)

// 在途租约的后端记录中途丢失（如后端重启/被清理）：Renew 必须把该 token
// 作为 missing 上报，且不复活；仍存活的占位不受影响。
func TestMemoryStore_RenewReportsMissingToken(t *testing.T) {
	ms := NewMemoryStore()
	now := time.Unix(5000, 0)
	clock := now
	ms.SetNow(func() time.Time { return clock })
	ctx := context.Background()
	ttl := 100 * time.Millisecond

	r, _ := ms.TryAcquire(ctx, AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 2, Token: "a", TTL: ttl})
	if r.Status != StatusAcquired {
		t.Fatalf("acquire a: %+v", r)
	}
	r, _ = ms.TryAcquire(ctx, AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 2, Token: "b", TTL: ttl})
	if r.Status != StatusAcquired {
		t.Fatalf("acquire b: %+v", r)
	}
	// 后端记录中途丢失：手动删除 a 的占位（模拟后端重启/键被清理）。
	ms.mu.Lock()
	delete(ms.conc["c"], "a")
	ms.mu.Unlock()

	missing, err := ms.Renew(ctx, []string{"c"}, []string{"a", "b"}, ttl)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if len(missing) != 1 || missing[0] != "a" {
		t.Fatalf("missing=%v want [a]", missing)
	}
	// b 必须已被续租，TTL 时点过后仍存活。
	clock = clock.Add(90 * time.Millisecond)
	if got := ms.Inflight("c"); got != 1 {
		t.Fatalf("inflight=%d want 1 (only b renewed)", got)
	}
}

// Reclaim：丢失的名额在有空位时原子占回，且不触碰速率令牌。
func TestMemoryStore_ReclaimRefillsLostSlot(t *testing.T) {
	ms := NewMemoryStore()
	now := time.Unix(6000, 0)
	clock := now
	ms.SetNow(func() time.Time { return clock })
	ctx := context.Background()
	ttl := 100 * time.Millisecond

	r, _ := ms.TryAcquire(ctx, AcquireRequest{RateKey: "r", ConcKey: "c", Burst: 1, MaxConcurrent: 1, Token: "a", TTL: ttl})
	if r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v", r)
	}
	ms.mu.Lock()
	delete(ms.conc["c"], "a")
	ms.mu.Unlock()
	// 空位已出现：若 Reclaim 误扣速率令牌，新请求速率仍会受限；这里先补占。
	st, err := ms.Reclaim(ctx, ReclaimRequest{ConcKey: "c", MaxConcurrent: 1, Token: "a", TTL: ttl})
	if err != nil || st != StatusAcquired {
		t.Fatalf("reclaim: st=%v err=%v want acquired", st, err)
	}
	if got := ms.Inflight("c"); got != 1 {
		t.Fatalf("inflight=%d want 1 after reclaim", got)
	}
	// 补占的名额立即生效：新请求必须被并发上限拒绝。
	r2, _ := ms.TryAcquire(ctx, AcquireRequest{RateKey: "r2", ConcKey: "c", Burst: 10, MaxConcurrent: 1, Token: "x", TTL: ttl})
	if r2.Status != StatusConcurrencyLimited {
		t.Fatalf("newcomer must be rejected after reclaim, got %+v", r2)
	}
}

// Reclaim 不得放大额度：租户键/共享键任一已满时整体不写入。
func TestMemoryStore_ReclaimRespectsCaps(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()
	ttl := time.Second
	// 场景 1：租户键被他人占满 → ConcurrencyLimited，且不写入任何键。
	r, _ := ms.TryAcquire(ctx, AcquireRequest{ConcKey: "c", MaxConcurrent: 1, Token: "other", TTL: ttl})
	if r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v", r)
	}
	st, err := ms.Reclaim(ctx, ReclaimRequest{ConcKey: "c", MaxConcurrent: 1, Token: "lost", TTL: ttl})
	if err != nil || st != StatusConcurrencyLimited {
		t.Fatalf("reclaim full tenant key: st=%v err=%v", st, err)
	}
	if got := ms.Inflight("c"); got != 1 {
		t.Fatalf("inflight=%d want 1 (reclaim must not write when full)", got)
	}
	if err := ms.Release(ctx, []string{"c"}, "other"); err != nil {
		t.Fatal(err)
	}

	// 场景 2：租户键有空位但共享池被占满 → SharedLimited，两键都不写入。
	r, _ = ms.TryAcquire(ctx, AcquireRequest{
		ConcKey: "c2", SharedKey: "sc", MaxConcurrent: 5, MaxShared: 1, Token: "s", TTL: ttl,
	})
	if r.Status != StatusAcquired {
		t.Fatalf("acquire shared: %+v", r)
	}
	st, err = ms.Reclaim(ctx, ReclaimRequest{
		ConcKey: "c2", SharedKey: "sc", MaxConcurrent: 5, MaxShared: 1, Token: "lost2", TTL: ttl,
	})
	if err != nil || st != StatusSharedLimited {
		t.Fatalf("reclaim full shared key: st=%v err=%v", st, err)
	}
	if got := ms.Inflight("c2"); got != 1 {
		t.Fatalf("tenant key inflight=%d want 1 (only holder s, reclaim wrote nothing)", got)
	}
	if got := ms.Inflight("sc"); got != 1 {
		t.Fatalf("shared inflight=%d want 1", got)
	}
}

// Redis（fakeredis，真实 Lua 路径）：记录丢失→Renew 上报→Reclaim 占回。
func TestRedisStore_RenewReclaimAfterBackendRecordLoss(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	st := newRedisStore(t, f)
	ctx := context.Background()
	ttl := 200 * time.Millisecond

	r, err := st.TryAcquire(ctx, AcquireRequest{
		RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
		MaxConcurrent: 1, Token: "a", TTL: ttl,
	})
	if err != nil || r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v %v", r, err)
	}
	// 模拟后端记录丢失。
	f.mu.Lock()
	delete(f.data[st.cl.key("cc")], "a")
	f.mu.Unlock()

	missing, err := st.Renew(ctx, []string{"cc"}, []string{"a"}, ttl)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if len(missing) != 1 || missing[0] != "a" {
		t.Fatalf("missing=%v want [a]", missing)
	}
	st2, err := st.Reclaim(ctx, ReclaimRequest{ConcKey: "cc", MaxConcurrent: 1, Token: "a", TTL: ttl})
	if err != nil || st2 != StatusAcquired {
		t.Fatalf("reclaim: st=%v err=%v", st2, err)
	}
	// 占回后新请求进不来。
	r2, _ := st.TryAcquire(ctx, AcquireRequest{
		RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
		MaxConcurrent: 1, Token: "b", TTL: ttl,
	})
	if r2.Status != StatusConcurrencyLimited {
		t.Fatalf("newcomer must be rejected after reclaim, got %+v", r2)
	}
	// 正常释放后续租不再上报丢失。
	if err := st.Release(ctx, []string{"cc"}, "a"); err != nil {
		t.Fatal(err)
	}
}

// 仅受共享池约束（无租户上限）的名额丢失：Reclaim 只补共享键。
func TestMemoryStore_ReclaimSharedOnly(t *testing.T) {
	ms := NewMemoryStore()
	ctx := context.Background()
	ttl := time.Second
	st, err := ms.Reclaim(ctx, ReclaimRequest{
		SharedKey: "sc", MaxShared: 1, Token: "a", TTL: ttl,
	})
	if err != nil || st != StatusAcquired {
		t.Fatalf("shared-only reclaim: st=%v err=%v", st, err)
	}
	if got := ms.Inflight("sc"); got != 1 {
		t.Fatalf("shared inflight=%d want 1", got)
	}
	// 共享池已满：补占另一个 token 必须受阻且不写入。
	st, err = ms.Reclaim(ctx, ReclaimRequest{
		SharedKey: "sc", MaxShared: 1, Token: "b", TTL: ttl,
	})
	if err != nil || st != StatusSharedLimited {
		t.Fatalf("full shared pool: st=%v err=%v", st, err)
	}
	if got := ms.Inflight("sc"); got != 1 {
		t.Fatalf("shared inflight=%d must stay 1", got)
	}
}
