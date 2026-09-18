package quota

import (
	"context"
	"testing"
	"time"
)

// fairGatewayConfig：共享组容量 2，a/b 各权重 1，无租户硬上限，
// 速率桶给足，使瓶颈只来自组内并发公平调度。
func fairGatewayConfig() *Config {
	return &Config{
		APIVersion: APIVersion,
		Version:    1,
		Mode:       ModeLocal,
		Degrade:    DegradeFailClosed,
		Tenants: map[string]map[string]TenantLimit{
			"a": {"api": {RatePerSec: 1e6, Burst: 1e6, MaxConcurrent: 0, Weight: 1}},
			"b": {"api": {RatePerSec: 1e6, Burst: 1e6, MaxConcurrent: 0, Weight: 1}},
		},
		Groups:       map[string]Group{"shared": {Name: "shared", Capacity: 2}},
		TenantGroups: map[string]map[string]string{"a": {"api": "shared"}, "b": {"api": "shared"}},
	}
}

func TestGatewayFairWaitThenAcquireOnRelease(t *testing.T) {
	cfg := fairGatewayConfig()
	g, err := NewGateway(cfg, Options{MaxWait: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// a 占满组容量 2
	var held []*Lease
	for i := 0; i < 2; i++ {
		l, err := g.Acquire(context.Background(), "a", "api")
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, l)
	}

	// b 在 goroutine 中申请：应公平等待，a 释放一个后获得
	got := make(chan *Lease, 1)
	errCh := make(chan error, 1)
	go func() {
		l, err := g.Acquire(context.Background(), "b", "api")
		if err != nil {
			errCh <- err
			return
		}
		got <- l
	}()

	// 确认 b 确实先进入等待
	time.Sleep(150 * time.Millisecond)
	select {
	case l := <-got:
		t.Fatalf("b should not acquire while capacity full: %v", l)
	case err := <-errCh:
		t.Fatalf("b errored before release: %v", err)
	default:
	}

	// a 释放一个名额（长连接正常结束）
	held[0].Release()

	select {
	case l := <-got:
		l.Release() // b 用完释放
	case err := <-errCh:
		t.Fatalf("b should acquire after release: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("b timed out waiting for fair slot")
	}
	held[1].Release()
}

func TestGatewayFairWaitDeadlineReturnsError(t *testing.T) {
	cfg := fairGatewayConfig()
	g, err := NewGateway(cfg, Options{MaxWait: 120 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	var held []*Lease
	for i := 0; i < 2; i++ {
		l, err := g.Acquire(context.Background(), "a", "api")
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, l)
	}
	_, err = g.Acquire(context.Background(), "b", "api")
	qe, ok := AsQuotaError(err)
	if !ok || qe.Reason != ReasonFairnessWait || qe.RetryIn <= 0 {
		t.Fatalf("expected fairness_wait with retry hint after deadline, got %v", err)
	}
	held[0].Release()
	held[1].Release()
}
