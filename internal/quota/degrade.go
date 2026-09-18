package quota

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Degrader 封装依赖不可用时的降级决策与本地兜底状态。
//
// fail-open: 放行请求，但仍按租户/接口的硬上限在本进程内做兜底计数，
//
//	避免配额依赖宕机期间单实例被彻底打爆；依赖恢复后本地计数即停。
//
// fail-closed: 一律返回 dependency_down 拒绝，保证绝不在状态不明时超额。
type Degrader struct {
	policy   DegradePolicy
	mu       sync.Mutex
	active   map[string]int64
	degraded atomic.Bool
}

// NewDegrader 根据策略创建降级器。
func NewDegrader(policy DegradePolicy) *Degrader {
	return &Degrader{policy: policy, active: make(map[string]int64)}
}

// Enter 在配额依赖调用失败时返回降级裁决。
// 返回 true 表示放行（fail-open，已占用一个本地兜底名额）；
// 返回 false 表示拒绝（fail-closed，err 非空）。
func (d *Degrader) Enter(ctx context.Context, tenant, iface string, hardCap int64) (bool, error) {
	d.degraded.Store(true)
	if d.policy == DegradeFailClosed {
		return false, newQuotaError(
			ReasonDependencyDown,
			"quota dependency unavailable and policy is fail-closed",
			500*time.Millisecond)
	}
	key := tenant + "\x00" + iface
	d.mu.Lock()
	defer d.mu.Unlock()
	if hardCap > 0 && d.active[key] >= hardCap {
		return false, newQuotaError(
			ReasonConcurrentLimit,
			"dependency down; local fallback concurrency cap reached",
			200*time.Millisecond)
	}
	d.active[key]++
	return true, nil
}

// Exit 归还 fail-open 期间占用的本地兜底名额（幂等、重复安全）。
func (d *Degrader) Exit(tenant, iface string) {
	key := tenant + "\x00" + iface
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active[key] > 0 {
		d.active[key]--
		if d.active[key] == 0 {
			delete(d.active, key)
		}
	}
}

// IsDegraded 返回当前是否正处于降级状态（供观测/健康检查）。
func (d *Degrader) IsDegraded() bool { return d.degraded.Load() }
