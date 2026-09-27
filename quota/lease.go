package quota

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lechandonga/quota-gateway/store"
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
	keys    []string // 本租约实际持有的后端占位键（降级放行的在途为空）
	tenant  string
	api     string
	ctx     context.Context
	release func() // 幂等释放（与调用方持有的 ReleaseFunc 相同）

	// mu 串行化“后台丢失补占（Reclaim）”与“正常结束的后端释放”：
	// 两者都要改同一个 token 的后端占位，必须互斥，防止
	// “Reclaim 已补回、Release 又误删”造成幽灵释放（提前归还）。
	mu sync.Mutex
	// lost 标记本租约的后端记录已被观测到丢失、正在补占中。
	// 为 true 时 Release 路径跳过后端删除（名额属于正在补占的本租约，
	// 删除会再次制造丢失）；补占成功后的 Release 仍正常走删除。
	lost bool
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

// get 返回指定 token 的租约（不存在返回 nil）。
func (r *leaseRegistry) get(token string) *leaseEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leases[token]
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
//
// 续租同时承担“名额中途丢失”的兜底：存储返回的 missing token 表示其
// 后端记录在某个持有键上已经不存在（后端抖动/重启/记录过期）。对仍登记
// 在本地、尚未释放的租约，按当前配置上限用 Reclaim 原子补占——
// 补占不扣速率令牌，且只有在租户键与共享键都有空位时才写入，
// 因此绝不放大额度；任一上限已满则下周期重试。
func (m *Manager) renewLeases() {
	entries := m.leases.snapshot()
	if len(entries) == 0 {
		return
	}
	groups := make(map[string][]*leaseEntry)
	pendingReclaim := make([]*leaseEntry, 0)
	for _, e := range entries {
		e.mu.Lock()
		lost := e.lost
		e.mu.Unlock()
		if lost {
			// 已进入补占流程的租约：后端记录不在了，不能再参与续租刷新
			// （HEXISTS 守卫本来也刷不到），本轮直接重试补占。
			pendingReclaim = append(pendingReclaim, e)
			continue
		}
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
		if k == "" {
			// 纯降级在途（不持有后端键），无需续租。
			continue
		}
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
		missing, err := m.st.Renew(ctx, es[0].keys, tokens, m.concTTL)
		cancel()
		if err != nil {
			// 续租失败不释放本地租约：下个周期重试；若后端持续不可用，
			// 占位最终在 TTL 到期后被后端惰性回收（有界）。
			m.logger.Printf("quota: renew leases failed keys=%q tokens=%d err=%v (将在 %s 后重试)",
				es[0].keys, len(tokens), err, m.renewInterval)
			continue
		}
		if len(missing) > 0 {
			m.handleMissingLeases(es[0].keys, missing)
		}
	}
	// 受阻/后端不可达而等待中的补占：每个续租周期重试一次。
	for _, e := range pendingReclaim {
		m.tryReclaim(e, false)
	}
}

// handleMissingLeases 处理续租批次上报的丢失 token：过滤掉本地已释放的
// 租约，对其余租约尝试进入补占流程（是否首次由 tryReclaim 在锁内判定，
// 避免续租批次与补占重试交错时重复计数）。
func (m *Manager) handleMissingLeases(keys []string, missing []string) {
	for _, tok := range missing {
		e := m.leases.get(tok)
		if e == nil {
			// 已正常释放的租约与续租批次并发交错：不是丢失事件，忽略。
			continue
		}
		m.tryReclaim(e, true)
	}
}

// reclaimLimits 按当前配置解析一条租约补占时使用的上限与占位键。
// 规则仍存在时返回 ruleExists=true；租户不设上限（MaxConcurrent<=0）时
// concKey 为空、仅共享键参与；规则已被热更新删除时 ruleExists=false。
func (m *Manager) reclaimLimits(e *leaseEntry) (concKey, sharedKey string, maxConc, maxShared int64, ruleExists bool) {
	cfg := m.cfg.Load()
	if cfg == nil {
		return "", "", 0, 0, false
	}
	if rule, ok := cfg.RuleFor(e.tenant, e.api); ok {
		ruleExists = true
		if rule.MaxConcurrent > 0 {
			concKey = "cc:" + e.tenant + ":" + e.api
			maxConc = rule.MaxConcurrent
		}
	}
	if cfg.GlobalMaxConcurrent > 0 {
		sharedKey = "sc:global"
		maxShared = cfg.GlobalMaxConcurrent
	}
	return
}

// heldConfiguredKeys 过滤出租约实际持有、且当前配置仍存在的占位键。
func heldConfiguredKeys(keys []string, concKey, sharedKey string) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k == concKey || k == sharedKey {
			out = append(out, k)
		}
	}
	return out
}

// dropPending / decPending 释放一批键上的补占预留（调用方不持有 e.mu）。
func (m *Manager) dropPending(keys []string) { m.decPending(keys) }

func (m *Manager) decPending(keys []string) {
	if len(keys) == 0 {
		return
	}
	m.pendingMu.Lock()
	for _, k := range keys {
		if m.pendingReclaim[k] > 0 {
			m.pendingReclaim[k]--
			if m.pendingReclaim[k] == 0 {
				delete(m.pendingReclaim, k)
			}
		}
	}
	m.pendingMu.Unlock()
}

// tryReclaim 按当前配置上限对一条丢失租约执行一次原子补占。firstAttempt
// 为 true 时表示由续租新上报的丢失事件（可能与既有补占重试重叠）；
// false 表示对已确认丢失租约的周期重试。
//
// 进入丢失态（置 lost、登记本地预留、计丢失指标）与“租约仍存活”复查都在
// e.mu 临界区内完成，保证每个丢失事件只计一次，并与正常释放严格互斥。
//
// 本地预留：补占完成前在本实例持有的每个占位键上占住 1 个额度，使本实例
// 的新获取不会先占掉等待补回的空位（本实例视角不超额）。
//
// 结果：
//   - 成功：在锁内清除丢失标记（避免与并发 Release 之间出现“已补回却被
//     跳过删除”的窗口），租约恢复为普通后端在途；
//   - 上限已满：保留标记与预留，下周期重试（有界自愈、不放大）；
//   - 后端不可达：同上，等待恢复；
//   - 规则已热更新删除：在途不再受配额约束，放弃补占、释放预留。
func (m *Manager) tryReclaim(e *leaseEntry, firstAttempt bool) {
	concKey, sharedKey, maxConc, maxShared, ruleExists := m.reclaimLimits(e)
	reserved := heldConfiguredKeys(e.keys, concKey, sharedKey)
	if !ruleExists {
		// 规则已被热更新删除：在途不再受配额约束。首次发现时从未登记
		// 预留；周期重试时释放旧键上登记过的预留，再清丢失标记。
		e.mu.Lock()
		if m.leases.get(e.token) != nil {
			if !firstAttempt {
				m.dropPendingLocked(e.keys)
			}
			e.lost = false
		}
		e.mu.Unlock()
		return
	}

	timeout := m.renewInterval
	if timeout > releaseTimeout {
		timeout = releaseTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	var (
		st        store.Status
		err       error
		succeeded bool
		firstSeen bool
	)
	e.mu.Lock()
	if m.leases.get(e.token) == nil {
		// 租约已结束：绝不补占。若此前已登记过预留（firstAttempt 再次
		// 观测到、或重试时恰被释放），在此兜底释放；正常情况下预留由
		// 释放路径的 skipBackendRelease 分支释放。
		drop := e.lost
		e.lost = false
		e.mu.Unlock()
		cancel()
		if drop {
			m.dropPending(reserved)
		}
		return
	}
	if !e.lost {
		// 首次确认丢失：在同一临界区进入补占态并登记预留。
		e.lost = true
		firstSeen = true
		m.addPendingLocked(reserved)
	}
	st, err = m.st.Reclaim(ctx, store.ReclaimRequest{
		ConcKey:       concKey,
		SharedKey:     sharedKey,
		MaxConcurrent: maxConc,
		MaxShared:     maxShared,
		Token:         e.token,
		TTL:           m.concTTL,
	})
	if err == nil && st == store.StatusAcquired {
		// 仍持锁时清除丢失标记：与“Reclaim 完成→标记清除”原子衔接，
		// 否则并发 Release 可能在间隙读到 lost=true 跳过删除，泄漏名额。
		e.lost = false
		succeeded = true
	}
	e.mu.Unlock()
	cancel()

	if firstSeen {
		if m.metrics != nil {
			m.metrics.IncLeaseLost(e.tenant, e.api)
		}
		m.logger.Printf("quota: lease slot lost in backend tenant=%s api=%s token=%s keys=%q (尝试按上限补占)",
			e.tenant, e.api, e.token, e.keys)
	}

	switch {
	case succeeded:
		m.dropPending(reserved)
		if m.metrics != nil {
			m.metrics.IncLeaseReclaimed(e.tenant, e.api)
		}
		m.logger.Printf("quota: lease slot reclaimed tenant=%s api=%s token=%s",
			e.tenant, e.api, e.token)
	case err == nil:
		// 上限已满：保留 lost=true 与预留，下周期重试（有界自愈、不放大）。
		if m.metrics != nil {
			m.metrics.IncLeaseReclaimBlocked(e.tenant, e.api)
		}
		m.logger.Printf("quota: lease reclaim blocked by cap tenant=%s api=%s token=%s status=%d (下周期重试)",
			e.tenant, e.api, e.token, st)
	default:
		// 后端不可达：保留状态，下周期重试。
		m.logger.Printf("quota: lease reclaim failed tenant=%s api=%s token=%s err=%v (下周期重试)",
			e.tenant, e.api, e.token, err)
	}
}

// addPendingLocked 批量登记补占预留。调用方持有 e.mu；这里另取 pendingMu，
// 锁序固定为 entry.mu → pendingMu（反向顺序在代码中不存在）。
func (m *Manager) addPendingLocked(keys []string) {
	m.pendingMu.Lock()
	for _, k := range keys {
		m.pendingReclaim[k]++
	}
	m.pendingMu.Unlock()
}

// dropPendingLocked 在 e.mu 临界区内释放预留。
func (m *Manager) dropPendingLocked(keys []string) {
	m.decPending(keys)
}
