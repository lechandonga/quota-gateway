package quota

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestGateway(t *testing.T, cfg *Config, opts ...func(*Options)) *Gateway {
	t.Helper()
	o := Options{Clock: NewFakeClock(time.Unix(1_700_000_000, 0)), MaxWait: 10 * time.Millisecond}
	for _, fn := range opts {
		fn(&o)
	}
	g, err := NewGateway(cfg, o)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestUnlimitedTenantNeverRejected(t *testing.T) {
	cfg := baseConfig()
	g := newTestGateway(t, cfg)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := g.Acquire(context.Background(), "unknown", "api")
			if err != nil {
				t.Errorf("unlimited tenant rejected: %v", err)
				return
			}
			l.Release()
			l.Release() // 重复释放必须安全
		}()
	}
	wg.Wait()
}

func TestRateLimitDistinguishableError(t *testing.T) {
	cfg := baseConfig() // t1/api: rate 10, burst 5
	g := newTestGateway(t, cfg)
	allowed := 0
	for i := 0; i < 10; i++ {
		l, err := g.Acquire(context.Background(), "t1", "api")
		if err == nil {
			allowed++
			l.Release()
			continue
		}
		qe, ok := AsQuotaError(err)
		if !ok || qe.Reason != ReasonRateLimited || qe.RetryIn <= 0 {
			t.Fatalf("expected rate_limited with retry hint, got %v", err)
		}
	}
	if allowed != 5 {
		t.Fatalf("burst allows exactly 5, got %d", allowed)
	}
}

func TestConcurrencyHardCap(t *testing.T) {
	cfg := baseConfig() // t1 maxConcurrent=3；burst 5 足够，不受速率影响
	g := newTestGateway(t, cfg)
	var leases []*Lease
	for i := 0; i < 3; i++ {
		l, err := g.Acquire(context.Background(), "t1", "api")
		if err != nil {
			t.Fatalf("lease %d: %v", i, err)
		}
		leases = append(leases, l)
	}
	_, err := g.Acquire(context.Background(), "t1", "api")
	qe, ok := AsQuotaError(err)
	if !ok || qe.Reason != ReasonConcurrentLimit {
		t.Fatalf("expected concurrent_limit, got %v", err)
	}
	// 释放一个后应立即恢复
	leases[0].Release()
	l, err := g.Acquire(context.Background(), "t1", "api")
	if err != nil {
		t.Fatalf("after release acquire should succeed: %v", err)
	}
	l.Release()
	for _, x := range leases[1:] {
		x.Release()
	}
}

func TestLeaseDoubleReleaseSafe(t *testing.T) {
	cfg := baseConfig()
	g := newTestGateway(t, cfg)
	l, err := g.Acquire(context.Background(), "t1", "api")
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	l.Release()
	l.Release()
	// 名额应已归还，可再次获取
	l2, err := g.Acquire(context.Background(), "t1", "api")
	if err != nil {
		t.Fatalf("slot should be available after release: %v", err)
	}
	l2.Release()
}

func TestContextCancelWhileFairWaiting(t *testing.T) {
	cfg := baseConfig() // group capacity 10，单租户上限 3，先占满
	g := newTestGateway(t, cfg)
	var held []*Lease
	for i := 0; i < 3; i++ {
		l, err := g.Acquire(context.Background(), "t1", "api")
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, l)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 客户端中断
	_, err := g.Acquire(ctx, "t1", "api")
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	for _, l := range held {
		l.Release()
	}
}
