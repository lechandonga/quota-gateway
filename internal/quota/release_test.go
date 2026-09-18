package quota

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestLongConnectionReleasedOnAllExitPaths 模拟长连接三种结束方式：
// 正常结束、处理超时、客户端中断；以及 panic 恢复后释放。
// 要求：无论哪条路径，并发占用都被精确归还，可再次获取。
func TestLongConnectionReleasedOnAllExitPaths(t *testing.T) {
	cfg := baseConfig() // t1 maxConcurrent=2, burst 5

	t.Run("normal", func(t *testing.T) {
		g := newTestGateway(t, cfg)
		l, err := g.Acquire(context.Background(), "t1", "api")
		if err != nil {
			t.Fatal(err)
		}
		l.Release()
		if _, err := g.Acquire(context.Background(), "t1", "api"); err != nil {
			t.Fatalf("slot should be reusable after normal release: %v", err)
		}
	})

	t.Run("handler-timeout", func(t *testing.T) {
		g := newTestGateway(t, cfg)
		l, err := g.Acquire(context.Background(), "t1", "api")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		<-ctx.Done() // 处理超时
		cancel()
		l.Release() // 超时清理路径仍须释放
		if _, err := g.Acquire(context.Background(), "t1", "api"); err != nil {
			t.Fatalf("slot reusable after timeout release: %v", err)
		}
	})

	t.Run("client-disconnect", func(t *testing.T) {
		g := newTestGateway(t, cfg)
		parent, cancel := context.WithCancel(context.Background())
		l, err := g.Acquire(parent, "t1", "api")
		if err != nil {
			t.Fatal(err)
		}
		cancel() // 客户端断开
		l.Release()
		if _, err := g.Acquire(context.Background(), "t1", "api"); err != nil {
			t.Fatalf("slot reusable after disconnect release: %v", err)
		}
	})

	t.Run("panic-recovery", func(t *testing.T) {
		g := newTestGateway(t, cfg)
		func() (err error) {
			l, aerr := g.Acquire(context.Background(), "t1", "api")
			if aerr != nil {
				t.Fatal(aerr)
			}
			defer l.Release() // defer 保证 panic 路径也释放
			defer func() { _ = recover() }()
			panic("boom")
		}()
		if _, err := g.Acquire(context.Background(), "t1", "api"); err != nil {
			t.Fatalf("slot reusable after panic+defer release: %v", err)
		}
	})
}

// TestNoLeakUnderMixedChaos 混合正常/超时/重复/乱序释放，最终占用必须归零。
func TestNoLeakUnderMixedChaos(t *testing.T) {
	s := newTestStore(t, nil)
	var wg sync.WaitGroup
	var tokens []LeaseToken
	var mu sync.Mutex
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rid := RequestID("chaos-" + itoaSafe(i))
			r, err := s.Admit(context.Background(), AdmitRequest{
				Tenant: "t1", Iface: "api", RequestID: rid, HardCap: 100,
			})
			if err != nil || !r.Admitted {
				return
			}
			mu.Lock()
			tokens = append(tokens, r.Token)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// 乱序 + 部分重复释放
	var rwg sync.WaitGroup
	for idx, tok := range tokens {
		rwg.Add(1)
		go func(idx int, tok LeaseToken) {
			defer rwg.Done()
			rid := RequestID("chaos-" + itoaSafe(idx))
			_, _ = s.Release(context.Background(), tok, rid)
			if idx%7 == 0 {
				_, _ = s.Release(context.Background(), tok, rid) // 重复
			}
		}(idx, tok)
	}
	rwg.Wait()

	if len(s.leases) != 0 {
		t.Fatalf("no leases may remain, got %d", len(s.leases))
	}
	if total := sumActive(s.active); total != 0 {
		t.Fatalf("no active slots may remain, got %d", total)
	}
}

func itoaSafe(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}

func sumActive(m map[string]int64) int64 {
	var t int64
	for _, v := range m {
		t += v
	}
	return t
}
