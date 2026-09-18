package quota

import (
	"context"
	"strconv"
	"testing"
)

// groupStoreConfig：A 权重 1、B 权重 3，组容量 4，无租户硬上限。
func groupStoreConfig() *Config {
	cfg := baseConfig()
	cfg.Tenants = map[string]map[string]TenantLimit{
		"a": {"api": {RatePerSec: 1e6, Burst: 1e6, MaxConcurrent: 0, Weight: 1}},
		"b": {"api": {RatePerSec: 1e6, Burst: 1e6, MaxConcurrent: 0, Weight: 3}},
	}
	cfg.TenantGroups = map[string]map[string]string{
		"a": {"api": "shared"},
		"b": {"api": "shared"},
	}
	return cfg
}

func newGroupStore(t *testing.T) *MemoryStore {
	s := NewMemoryStore(SystemClock())
	if err := s.SyncConfig(context.Background(), groupStoreConfig()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func groupAdmit(s *MemoryStore, tenant, rid string) AdmitResult {
	w := 1.0
	if tenant == "b" {
		w = 3
	}
	r, err := s.Admit(context.Background(), AdmitRequest{
		Tenant: tenant, Iface: "api", RequestID: RequestID(rid),
		GroupName: "shared", GroupCapacity: 4, Weight: w,
	})
	if err != nil {
		panic(err)
	}
	return r
}

func TestGroupIdleBorrowAndReturn(t *testing.T) {
	s := newGroupStore(t)

	// A 无请求，B 借用闲置容量直到打满 4
	var bTokens []LeaseToken
	for i := 0; i < 4; i++ {
		rid := "b-" + strconv.Itoa(i)
		r := groupAdmit(s, "b", rid)
		if !r.Admitted {
			t.Fatalf("B should borrow idle slot %d, got %s", i, r.Reason)
		}
		bTokens = append(bTokens, r.Token)
	}

	// A 此时竞争：容量满，必须排队（不能被 B 长期饿死）
	if r := groupAdmit(s, "a", "a-wait-1"); r.Admitted || r.Reason != ReasonFairnessWait {
		t.Fatalf("A must fair-wait behind borrowing B, got %+v", r)
	}

	// B 释放一个名额：A（低于其保证份额）下一轮排队尝试应获得
	if _, err := s.Release(context.Background(), bTokens[0], "b-0"); err != nil {
		t.Fatal(err)
	}
	rA2 := groupAdmit(s, "a", "a-wait-2")
	if !rA2.Admitted {
		t.Fatalf("A must reclaim share after B releases, got %s", rA2.Reason)
	}
	if got := s.active["a\x00api"]; got != 1 {
		t.Fatalf("A active should be 1, got %d", got)
	}
}

func TestGroupWeightedSteadyState(t *testing.T) {
	s := newGroupStore(t)

	// 阶段 1：只有 B，借满全部 4
	var bTokens []LeaseToken
	for i := 0; i < 4; i++ {
		r := groupAdmit(s, "b", "b-hold-"+strconv.Itoa(i))
		if !r.Admitted {
			t.Fatalf("B borrow fill %d: %s", i, r.Reason)
		}
		bTokens = append(bTokens, r.Token)
	}

	// 阶段 2：A 加入并持续排队，B 持有的名额随释放回流。
	// 逐步释放 B 的名额并让 A 申请，直到 A 拿满其加权份额 1。
	aHeld := 0
	released := 0
	for aHeld < 1 {
		if released >= len(bTokens) {
			t.Fatal("ran out of B tokens before A reached its share")
		}
		if _, err := s.Release(context.Background(), bTokens[released], RequestID("rel-"+strconv.Itoa(released))); err != nil {
			t.Fatal(err)
		}
		released++
		r := groupAdmit(s, "a", "a-join-"+strconv.Itoa(aHeld))
		if r.Admitted {
			aHeld++
		}
	}
	if aHeld != 1 {
		t.Fatalf("A weighted share must be 1, got %d", aHeld)
	}
	if got := s.active["b\x00api"]; got != 3 {
		t.Fatalf("B should retain its weighted share 3, got %d", got)
	}

	// 阶段 3：稳态 1:3，容量已满，双方再申请都必须等待
	if r := groupAdmit(s, "a", "a-extra"); r.Admitted {
		t.Fatal("A extra must fair-wait at weighted steady state")
	}
	if r := groupAdmit(s, "b", "b-extra"); r.Admitted {
		t.Fatal("B extra must fair-wait at weighted steady state")
	}
}
