package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newRedisStore(t *testing.T, f *fakeRedis) *RedisStore {
	t.Helper()
	st, err := NewRedisStore(RedisOptions{Addr: f.addr(), KeyPrefix: "t", DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestRedisStore_PingAndAcquire(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	st := newRedisStore(t, f)
	ctx := context.Background()
	if err := st.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	req := AcquireRequest{
		RateKey: "rt", ConcKey: "cc", Rate: 2, Burst: 2,
		MaxConcurrent: 2, Token: "tok1", TTL: time.Second,
	}
	r, err := st.TryAcquire(ctx, req)
	if err != nil || r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v %v", r, err)
	}
	r, err = st.TryAcquire(ctx, req)
	if err != nil || r.Status != StatusAcquired {
		t.Fatalf("acquire2: %+v %v", r, err)
	}
	// 第三次：速率桶已空（burst=2 已耗）。
	r, _ = st.TryAcquire(ctx, req)
	if r.Status != StatusRateLimited {
		t.Fatalf("status=%d want rate limited", r.Status)
	}
}

// TestRedisStore_MultiInstanceConsistency 是多实例核心保证：
// 多个 RedisStore 实例共享同一后端，全局占位总数绝不能超过共享池上限。
func TestRedisStore_MultiInstanceConsistency(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	const instances, goroutines, maxShared = 5, 60, 7
	var stores []*RedisStore
	for i := 0; i < instances; i++ {
		stores = append(stores, newRedisStore(t, f))
	}
	var wg sync.WaitGroup
	granted := make(chan struct{}, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			st := stores[i%instances]
			req := AcquireRequest{
				RateKey:   "rt:" + "x",
				ConcKey:   "cc:" + "x",
				SharedKey: "shared",
				Rate:      0, Burst: 1,
				MaxConcurrent: goroutines, // 租户上限宽松，只压共享池
				MaxShared:     maxShared,
				Token:         "tok" + itoaX(i),
				TTL:           0,
			}
			r, err := st.TryAcquire(context.Background(), req)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if r.Status == StatusAcquired {
				granted <- struct{}{}
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
	if n != maxShared {
		t.Fatalf("granted across %d instances=%d want exactly %d (no amplification)",
			instances, n, maxShared)
	}
}

func TestRedisStore_ReleaseIdempotent(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	st := newRedisStore(t, f)
	req := AcquireRequest{
		RateKey: "rt", ConcKey: "cc", SharedKey: "shared",
		Rate: 0, Burst: 1, MaxConcurrent: 10, MaxShared: 1,
		Token: "tok", TTL: 0,
	}
	if r, _ := st.TryAcquire(context.Background(), req); r.Status != StatusAcquired {
		t.Fatalf("acquire: %+v", r)
	}
	if r, _ := st.TryAcquire(context.Background(), req); r.Status != StatusSharedLimited {
		t.Fatalf("want shared limited, got %+v", r)
	}
	keys := []string{"cc", "shared"}
	for i := 0; i < 3; i++ {
		if err := st.Release(context.Background(), keys, "tok"); err != nil {
			t.Fatal(err)
		}
	}
	if r, _ := st.TryAcquire(context.Background(), req); r.Status != StatusAcquired {
		t.Fatalf("after release should acquire, got %+v", r)
	}
}

// TestRedisStore_NoScriptFallback 验证脚本缓存丢失时自动 EVAL 回退。
func TestRedisStore_NoScriptFallback(t *testing.T) {
	f := startFakeRedis(t)
	defer f.close()
	st := newRedisStore(t, f)
	req := AcquireRequest{
		RateKey: "rt", ConcKey: "cc", Rate: 0, Burst: 1,
		MaxConcurrent: 1, Token: "t", TTL: 0,
	}
	if _, err := st.TryAcquire(context.Background(), req); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	// 强制下一次 EVALSHA 返回 NOSCRIPT。
	f.mu.Lock()
	f.failNextEvalSHA = 1
	f.mu.Unlock()
	if err := st.Release(context.Background(), []string{"cc"}, "t"); err != nil {
		t.Fatalf("release should transparently fallback on NOSCRIPT: %v", err)
	}
	// 回退后 loaded 标志被清，下一次调用应再次成功（重新 EVAL/加载）。
	if r, _ := st.TryAcquire(context.Background(), req); r.Status != StatusAcquired {
		t.Fatalf("after NOSCRIPT recovery: %+v", r)
	}
}

func TestRedisStore_ConnectFailure(t *testing.T) {
	// 监听后立即关闭，保证地址不可用。
	ln := mustListen(t)
	addr := ln.Addr().String()
	_ = ln.Close()
	if _, err := NewRedisStore(RedisOptions{Addr: addr, DialTimeout: 100 * time.Millisecond}); err == nil {
		t.Fatal("expected connection failure")
	}
}

func itoaX(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
