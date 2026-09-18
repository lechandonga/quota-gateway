package quota

import (
	"context"
	"testing"
	"time"
)

// TestRateRecoveryOverTime 用假时钟验证被限流后，令牌随时间精确恢复，
// 到达建议重试时间后请求可再次通过（整体吞吐稳定、不会永久封禁）。
func TestRateRecoveryOverTime(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	clock := NewFakeClock(start)
	cfg := baseConfig() // rate 10/s, burst 5
	l := cfg.Tenants["t1"]["api"]
	l.MaxConcurrent = 0 // 本测试只验证速率，不受并发限制
	cfg.Tenants["t1"]["api"] = l
	g, err := NewGateway(cfg, Options{Clock: clock, MaxWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	for i := 0; i < 5; i++ {
		if _, err := g.Acquire(context.Background(), "t1", "api"); err != nil {
			t.Fatalf("burst %d: %v", i, err)
		}
	}
	_, err = g.Acquire(context.Background(), "t1", "api")
	qe, _ := AsQuotaError(err)
	if qe == nil || qe.Reason != ReasonRateLimited {
		t.Fatalf("expected rate_limited, got %v", err)
	}
	retry := qe.RetryIn

	// 推进假时钟到达建议重试时刻：应恰好恢复 1 个令牌
	clock.Advance(retry + time.Millisecond)
	lease, err := g.Acquire(context.Background(), "t1", "api")
	if err != nil {
		t.Fatalf("request must succeed after retry window elapsed: %v", err)
	}
	lease.Release()

	// 紧接的请求仍应被限流（只恢复了一个）
	if _, err := g.Acquire(context.Background(), "t1", "api"); err == nil {
		t.Fatal("only one token should have recovered")
	}
}

// TestConfigHotReloadChangesLimits 验证热更新立即改变限额，非法更新不影响现状。
func TestConfigHotReloadChangesLimits(t *testing.T) {
	cfg := baseConfig() // t1 burst 5
	g := newTestGateway(t, cfg)

	updated := baseConfig()
	updated.Version = 2
	l := updated.Tenants["t1"]["api"]
	l.Burst = 2
	l.RatePerSec = 1
	updated.Tenants["t1"]["api"] = l
	if err := g.ReplaceConfig(context.Background(), updated); err != nil {
		t.Fatalf("hot reload: %v", err)
	}
	allowed := 0
	for i := 0; i < 5; i++ {
		if _, err := g.Acquire(context.Background(), "t1", "api"); err == nil {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("after hot reload burst must be 2, allowed=%d", allowed)
	}

	// 非法的下一版配置：拒绝且当前仍是 v2
	bad := baseConfig()
	bad.Version = 3
	bad.Tenants["t1"]["api"] = TenantLimit{RatePerSec: -1, Burst: 1}
	if err := g.ReplaceConfig(context.Background(), bad); err == nil {
		t.Fatal("invalid hot reload must be rejected")
	}
	if g.CurrentConfig().Version != 2 {
		t.Fatalf("current config must remain v2, got %d", g.CurrentConfig().Version)
	}
}
