package quota

import (
	"context"
	"testing"
	"time"
)

// 长连接进行中发生热更新：在途名额不得被误回收或重复占用；
// 非法配置被拒绝且在用配置不受影响。
func TestHotUpdate_DuringLongConnection(t *testing.T) {
	v1 := &Config{
		Version: "1.0",
		Degrade: DegradeDeny,
		Rules: []Rule{
			{Tenant: "t1", API: "/long", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
		},
	}
	m, ms := newLeaseManager(t, v1, 150*time.Millisecond, 40*time.Millisecond)
	ctx := context.Background()

	// 长连接在途（远超 TTL，靠续租保持）。
	hold, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer hold()
	time.Sleep(300 * time.Millisecond) // 已超过 2 倍 TTL

	// 热更新：放宽到 2。在途名额不得被误回收。
	v2 := &Config{
		Version: "1.1",
		Degrade: DegradeDeny,
		Rules: []Rule{
			{Tenant: "t1", API: "/long", Rate: 0, Burst: 1, MaxConcurrent: 2, Weight: 1},
		},
	}
	if err := m.UpdateConfig(v2); err != nil {
		t.Fatalf("update to v2: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // 更新后再跨一个 TTL
	if got := ms.Inflight("cc:t1:/long"); got != 1 {
		t.Fatalf("in-flight slot lost across hot update: inflight=%d", got)
	}

	// 新上限生效：可再进 1 个，但第 3 个必须被拒（不重复占用、不放大）。
	rel2, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire under new limit: %v", err)
	}
	defer rel2()
	if _, err := m.Acquire(ctx, "t1", "/long"); err == nil {
		t.Fatal("third acquire must be rejected under limit 2")
	}

	// 非法配置被拒绝，在用配置不受影响。
	bad := &Config{Version: "2.0", Rules: v2.Rules}
	if err := m.UpdateConfig(bad); err == nil {
		t.Fatal("incompatible version must be rejected")
	}
	if got := m.ConfigVersion(); got != "1.1" {
		t.Fatalf("active config version=%q want 1.1 after rejected update", got)
	}
	if _, err := m.Acquire(ctx, "t1", "/long"); err == nil {
		t.Fatal("limit must still be enforced after rejected update")
	}
}

// 热更新删除规则：在途长连接不受影响且能正常释放；新请求直接放行。
func TestHotUpdate_RemoveRuleKeepsInFlightSafe(t *testing.T) {
	v1 := &Config{
		Version: "1.0",
		Degrade: DegradeDeny,
		Rules: []Rule{
			{Tenant: "t1", API: "/long", Rate: 0, Burst: 1, MaxConcurrent: 1, Weight: 1},
		},
	}
	m, ms := newLeaseManager(t, v1, 150*time.Millisecond, 40*time.Millisecond)
	ctx := context.Background()

	hold, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// 删除全部规则。
	if err := m.UpdateConfig(&Config{Version: "1.1", Degrade: DegradeDeny}); err != nil {
		t.Fatalf("update: %v", err)
	}
	// 在途连接继续被续租（名额仍在），释放后归零、不泄漏。
	time.Sleep(200 * time.Millisecond)
	if got := ms.Inflight("cc:t1:/long"); got != 1 {
		t.Fatalf("in-flight slot lost after rule removal: inflight=%d", got)
	}
	hold()
	time.Sleep(100 * time.Millisecond)
	if got := ms.Inflight("cc:t1:/long"); got != 0 {
		t.Fatalf("slot leaked after release: inflight=%d", got)
	}
	// 无规则：新请求直接放行且不占名额。
	rel, err := m.Acquire(ctx, "t1", "/long")
	if err != nil {
		t.Fatalf("unruled acquire: %v", err)
	}
	rel()
}
