package quota

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// 本文件实现长连接的租约（lease）保活与回收：
//
//   - 每次 Acquire 成功都会登记一条租约（token + 释放键 + 调用方 ctx）；
//   - 管理器内唯一一个后台循环周期性地：
//     a) 续租：把仍存活租约的占位过期时间延长（只要请求在途、实例存活，
//     名额就一直被占用，不会因 TTL 到期被提前回收）；
//     b) 回收：发现租约 ctx 已取消（如客户端断连而 handler 仍未返回）时，
//     触发与正常结束完全相同的幂等释放。
//   - 实例崩溃/强杀：续租停止，遗留占位在 ConcurrencyTTL 内有界回收。
//
// 协程/定时器数量与连接数无关：整个 Manager 只有一个后台 goroutine
// 和一个 ticker；租约表大小即当前在途请求数，受配额上限约束。

// leaseEntry 是一条在途租约。
type leaseEntry struct {
	token   string
	keys    []string // 占位键（租户并发键 + 可选共享池键）
	ctx     context.Context
	release func() // 幂等释放（与调用方持有的 ReleaseFunc 相同）
}

// leaseRegistry 是 Manager 的租约登记表。
type leaseRegistry struct {
	mu     sync.Mutex
	leases map[string]*leaseEntry // token -> entry
}

func newLeaseRegistry() *leaseRegistry {
	return &leaseRegistry{leases: make(map[string]*leaseEntry)}
}

func (r *leaseRegistry) add(e *leaseEntry) {
	r.mu.Lock()
	r.leases[e.token] = e
	r.mu.Unlock()
}

func (r *leaseRegistry) remove(token string) {
	r.mu.Lock()
	delete(r.leases, token)
	r.mu.Unlock()
}

// snapshot 返回当前租约的稳定快照（调用方不持有锁）。
func (r *leaseRegistry) snapshot() []*leaseEntry {
	r.mu.Lock()
	out := make([]*leaseEntry, 0, len(r.leases))
	for _, e := range r.leases {
		out = append(out, e)
	}
	r.mu.Unlock()
	return out
}

// len 返回当前租约数（测试与观测用）。
func (r *leaseRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.leases)
}

// leaseLoop 是 Manager 唯一的后台循环：按 reapInterval  ticking，
// 每轮先回收 ctx 已取消的租约，到点再批量续租。
func (m *Manager) leaseLoop() {
	defer m.loopWg.Done()
	tick := time.NewTicker(m.reapInterval)
	defer tick.Stop()
	nextRenew := m.now().Add(m.renewInterval)
	for {
		select {
		case <-m.loopStop:
			return
		case now := <-tick.C:
			m.reapCanceledLeases()
			if !now.Before(nextRenew) {
				m.renewLeases()
				nextRenew = now.Add(m.renewInterval)
			}
		}
	}
}

// reapCanceledLeases 释放 ctx 已取消的租约（客户端断连等）。
// 释放在独立的单飞 goroutine 中执行：后端缓慢/不可用时不会阻塞
// 后台循环的续租职责；单飞标志保证任意时刻至多一个回收批次在飞，
// 未收完的租约下一轮继续（释放函数幂等，重叠安全）。
func (m *Manager) reapCanceledLeases() {
	var canceled []*leaseEntry
	for _, e := range m.leases.snapshot() {
		if e.ctx != nil && e.ctx.Err() != nil {
			canceled = append(canceled, e)
		}
	}
	if len(canceled) == 0 {
		return
	}
	m.reapMu.Lock()
	if m.reapRunning {
		m.reapMu.Unlock()
		return
	}
	m.reapRunning = true
	m.reapMu.Unlock()
	go func() {
		defer func() {
			m.reapMu.Lock()
			m.reapRunning = false
			m.reapMu.Unlock()
		}()
		for _, e := range canceled {
			e.release()
		}
	}()
}

// renewLeases 批量续租：按释放键集合分组，每组一次存储调用，
// 调用次数与“键集合种类”同级，与连接数无关。
func (m *Manager) renewLeases() {
	entries := m.leases.snapshot()
	if len(entries) == 0 {
		return
	}
	groups := make(map[string][]*leaseEntry)
	for _, e := range entries {
		k := strings.Join(e.keys, "\x00")
		groups[k] = append(groups[k], e)
	}
	// 稳定遍历顺序，便于测试与日志可解释。
	sortKeys := make([]string, 0, len(groups))
	for k := range groups {
		sortKeys = append(sortKeys, k)
	}
	sort.Strings(sortKeys)
	for _, k := range sortKeys {
		es := groups[k]
		tokens := make([]string, 0, len(es))
		for _, e := range es {
			tokens = append(tokens, e.token)
		}
		// 单次续租的存储访问 capped：后端缓慢时后台循环仍能及时
		// 回到回收职责，避免健康租约因续租被拖而误过期。
		timeout := m.renewInterval
		if timeout > releaseTimeout {
			timeout = releaseTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := m.st.Renew(ctx, es[0].keys, tokens, m.concTTL)
		cancel()
		if err != nil {
			// 续租失败不释放本地租约：下个周期重试；若后端持续不可用，
			// 占位最终在 TTL 到期后被后端惰性回收（有界）。
			m.logger.Printf("quota: renew leases failed keys=%q tokens=%d err=%v (将在 %s 后重试)",
				es[0].keys, len(tokens), err, m.renewInterval)
		}
	}
}
