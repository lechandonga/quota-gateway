package quota

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T, clock Clock) *MemoryStore {
	t.Helper()
	if clock == nil {
		clock = SystemClock()
	}
	s := NewMemoryStore(clock)
	t.Cleanup(func() { _ = s.Close() })
	if err := s.SyncConfig(context.Background(), baseConfig()); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStoreHardCapUnderContention(t *testing.T) {
	s := newTestStore(t, nil)
	const goroutines = 64
	var wg sync.WaitGroup
	var granted int64
	var mu sync.Mutex
	errCount := 0
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reqID := RequestID("r-" + strconv.Itoa(i))
			res, err := s.Admit(context.Background(), AdmitRequest{
				Tenant: "t1", Iface: "api",
				RequestID: reqID,
				HardCap:   3,
			})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			if res.Admitted {
				granted++
			} else {
				errCount++
			}
			mu.Unlock()
			// 不立即释放：验证同一时刻占用绝不超过上限
			time.Sleep(20 * time.Millisecond)
			if res.Admitted {
				if _, err := s.Release(context.Background(), res.Token, reqID); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	if granted != 3 {
		t.Fatalf("exactly 3 concurrent grants expected, got %d (rejected=%d)", granted, errCount)
	}
}

func TestStoreIdempotentAdmitRetries(t *testing.T) {
	s := newTestStore(t, nil)
	req := AdmitRequest{Tenant: "t1", Iface: "api", RequestID: "same-id", HardCap: 3}
	first, err := s.Admit(context.Background(), req)
	if err != nil || !first.Admitted {
		t.Fatalf("first admit: %+v %v", first, err)
	}
	// 网络重试：完全相同的请求必须返回同一个凭证，不得重复占名额
	second, err := s.Admit(context.Background(), req)
	if err != nil || !second.Admitted || second.Token != first.Token {
		t.Fatalf("idempotent retry must return same token: first=%s second=%s err=%v",
			first.Token, second.Token, err)
	}
	if got := s.active["t1\x00api"]; got != 1 {
		t.Fatalf("retries must not double-count, active=%d", got)
	}
	if _, err := s.Release(context.Background(), first.Token, "same-id"); err != nil {
		t.Fatal(err)
	}
	// 相同请求 ID 再次准入不得获得新名额
	third, _ := s.Admit(context.Background(), req)
	if third.Admitted {
		t.Fatal("reused request id after release must not grant a new slot")
	}
}

func TestStoreReleaseOutOfOrderAndUnknown(t *testing.T) {
	s := newTestStore(t, nil)
	r1, _ := s.Admit(context.Background(), AdmitRequest{Tenant: "t1", Iface: "api", RequestID: "a", HardCap: 5})
	r2, _ := s.Admit(context.Background(), AdmitRequest{Tenant: "t1", Iface: "api", RequestID: "b", HardCap: 5})
	// 未知凭证
	res, err := s.Release(context.Background(), "bogus", "x")
	if err != nil || res.Released {
		t.Fatal("unknown token release must be no-op")
	}
	// 乱序：先放第二个
	if _, err := s.Release(context.Background(), r2.Token, "b"); err != nil {
		t.Fatal(err)
	}
	// 重复释放 r2
	res, _ = s.Release(context.Background(), r2.Token, "b2")
	if res.Released {
		t.Fatal("double release must report released=false and be harmless")
	}
	// 再放第一个
	if _, err := s.Release(context.Background(), r1.Token, "a"); err != nil {
		t.Fatal(err)
	}
	if len(s.active) != 0 || len(s.leases) != 0 {
		t.Fatalf("all leases must be gone, active=%v leases=%d", s.active, len(s.leases))
	}
}

// TestStoreRateDedupNoDoubleCharge 验证相同 dedupKey 的“超时重放”只扣一次令牌。
func TestStoreRateDedupNoDoubleCharge(t *testing.T) {
	cfg := baseConfig()
	lim := cfg.Tenants["t1"]["api"]
	lim.RatePerSec = 1 // 极慢恢复
	lim.Burst = 2
	lim.MaxConcurrent = 0
	cfg.Tenants["t1"]["api"] = lim
	s := NewMemoryStore(SystemClock())
	defer s.Close()
	if err := s.SyncConfig(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	key := RateKey{Tenant: "t1", Iface: "api"}
	first, err := s.TryTakeRate(context.Background(), RateDeduction{Key: key, Want: 1, DedupKey: "dedup-1"})
	if err != nil || !first.Allowed {
		t.Fatalf("first take: %+v %v", first, err)
	}
	// 模拟响应丢失后的重放：必须返回首次结果，不二次扣减
	replay, err := s.TryTakeRate(context.Background(), RateDeduction{Key: key, Want: 1, DedupKey: "dedup-1"})
	if err != nil || !replay.Allowed || replay.Remaining != first.Remaining {
		t.Fatalf("replay must return cached result without extra charge: first=%+v replay=%+v", first, replay)
	}
	// 全新逻辑请求使用第二个令牌
	second, err := s.TryTakeRate(context.Background(), RateDeduction{Key: key, Want: 1, DedupKey: "dedup-2"})
	if err != nil || !second.Allowed {
		t.Fatalf("second logical request: %+v %v", second, err)
	}
	// 第三个逻辑请求必须被限流（burst=2 已用完）
	third, err := s.TryTakeRate(context.Background(), RateDeduction{Key: key, Want: 1, DedupKey: "dedup-3"})
	if err != nil || third.Allowed {
		t.Fatalf("third request must be rate limited, got %+v %v", third, err)
	}
}
