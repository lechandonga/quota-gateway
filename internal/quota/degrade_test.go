package quota

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// brokenStore 始终返回错误，模拟配额依赖不可用。
type brokenStore struct{ closed atomic.Bool }

func (b *brokenStore) SyncConfig(context.Context, *Config) error { return nil }
func (b *brokenStore) TryTakeRate(context.Context, RateDeduction) (RateResult, error) {
	return RateResult{}, context.DeadlineExceeded
}
func (b *brokenStore) Admit(context.Context, AdmitRequest) (AdmitResult, error) {
	return AdmitResult{}, context.DeadlineExceeded
}
func (b *brokenStore) Release(context.Context, LeaseToken, RequestID) (ReleaseResult, error) {
	return ReleaseResult{}, context.DeadlineExceeded
}
func (b *brokenStore) Close() error { b.closed.Store(true); return nil }

func gatewayWithBrokenStore(t *testing.T, policy DegradePolicy) *Gateway {
	cfg := baseConfig()
	cfg.Degrade = policy
	g, err := NewGateway(cfg, Options{Store: &brokenStore{}, Degrade: policy, MaxWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestDegradeFailClosedRejects(t *testing.T) {
	g := gatewayWithBrokenStore(t, DegradeFailClosed)
	_, err := g.Acquire(context.Background(), "t1", "api")
	qe, ok := AsQuotaError(err)
	if !ok || qe.Reason != ReasonDependencyDown {
		t.Fatalf("fail-closed must return dependency_down, got %v", err)
	}
	if !g.IsDegraded() {
		t.Fatal("degraded flag must be observable")
	}
}

func TestDegradeFailOpenAllowsButCaps(t *testing.T) {
	g := gatewayWithBrokenStore(t, DegradeFailOpen)
	var held []*Lease
	// 本地兜底仍尊重租户硬上限 3
	for i := 0; i < 3; i++ {
		l, err := g.Acquire(context.Background(), "t1", "api")
		if err != nil {
			t.Fatalf("fail-open should admit within cap: %v", err)
		}
		held = append(held, l)
	}
	_, err := g.Acquire(context.Background(), "t1", "api")
	if err == nil {
		t.Fatal("fallback local cap must still reject excess")
	}
	held[0].Release()
	if _, err := g.Acquire(context.Background(), "t1", "api"); err != nil {
		t.Fatalf("after release, fallback slot reusable: %v", err)
	}
	for _, l := range held[1:] {
		l.Release()
	}
}

func TestDegradeUnlimitedStillPassesWhenDown(t *testing.T) {
	g := gatewayWithBrokenStore(t, DegradeFailClosed)
	// 未配置限额的租户不依赖存储，fail-closed 下也应放行
	l, err := g.Acquire(context.Background(), "nobody", "api")
	if err != nil {
		t.Fatalf("unconfigured tenant must pass even with dependency down: %v", err)
	}
	l.Release()
}
