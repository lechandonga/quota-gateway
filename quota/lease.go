package quota

import (
	"context"
	"sync"
	"time"

	"github.com/lechandonga/quota-gateway/store"
)

// 续租操作的单次超时；默认续租间隔的 2 倍，封顶 2s。
// 即便后端短暂抖动也能在下个周期重试，只有持续失联超过 TTL 才会丢约。
const maxRenewCallTimeout = 2 * time.Second

// leaseHold 是一个“已在后端占位、请求仍在处理中”的凭证。
type leaseHold struct {
	token    string
	keys     []string // 租户并发键 [+ 共享池键]
	tenant   string
	api      string
	ttl      time.Duration
	shared   bool   // 是否占用了共享容量池（释放时需唤醒公平队列）
	localKey string // 非空表示同时占用了本地并发闸门名额
	lost     bool   // 凭证已在后端丢失（TTL 回收/续租确认失败）
}

// leaseRegistry 记录本实例当前持有的全部后端占位。
//
// 唯一的后台续租 goroutine 周期性为所有在途凭证续租：因此无论请求持续
// 多久（分钟、小时级长连接），只要实例存活，占位就不会被 TTL 提前回收；
// 实例崩溃后续租停止，遗留占位在一个 TTL 内被后端惰性回收。
type leaseRegistry struct {
	mu     sync.Mutex
	holds  map[string]*leaseHold
	closed bool
}

func newLeaseRegistry() *leaseRegistry {
	return &leaseRegistry{holds: make(map[string]*leaseHold)}
}

func (r *leaseRegistry) add(h *leaseHold) {
	r.mu.Lock()
	r.holds[h.token] = h
	r.mu.Unlock()
}

// remove 摘除凭证登记（幂等）；返回被摘除的 hold（可能为 nil）。
func (r *leaseRegistry) remove(token string) *leaseHold {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.holds[token]
	delete(r.holds, token)
	return h
}

func (r *leaseRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.holds)
}

func (r *leaseRegistry) snapshot() []*leaseHold {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*leaseHold, 0, len(r.holds))
	for _, h := range r.holds {
		out = append(out, h)
	}
	return out
}

// has 报告凭证是否仍登记在册（用于续租返回后的竞争复核）。
func (r *leaseRegistry) has(token string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.holds[token]
	return ok
}

func (r *leaseRegistry) markLost(token string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.holds[token]
	if h == nil || h.lost {
		return false
	}
	h.lost = true
	return true
}

// renewer 是单 goroutine 的周期续租器。
type renewer struct {
	reg      *leaseRegistry
	st       renewStore
	ttl      func() time.Duration // 当前生效 TTL（热更新后变化）
	interval func() time.Duration // 续租周期（测试可调）
	logger   Logger
	metrics  renewMetrics
	onLost   func(h *leaseHold)

	stop chan struct{}
	wg   sync.WaitGroup
}

type renewStore interface {
	Renew(ctx context.Context, keys []string, token string, ttl time.Duration) (bool, error)
	// RenewAll 批量续租多个凭证：要么全部续期成功（返回 nil），
	// 要么返回未能续期的凭证集合（缺失/过期）。内存实现据此在一把锁内
	// 原子完成，避免数千凭证逐个续租时单轮扫描超过短 TTL。
	RenewAll(ctx context.Context, items []store.RenewItem) (missing []string, err error)
	Release(ctx context.Context, keys []string, token string) error
}

type renewMetrics interface {
	IncLeaseLost()
	IncRenewFailed()
}

func startRenewer(reg *leaseRegistry, st renewStore, ttl, interval func() time.Duration,
	logger Logger, metrics renewMetrics, onLost func(*leaseHold)) *renewer {
	if logger == nil {
		logger = discardLogger{}
	}
	rw := &renewer{
		reg: reg, st: st, ttl: ttl, interval: interval,
		logger: logger, metrics: metrics, onLost: onLost,
		stop: make(chan struct{}),
	}
	rw.wg.Add(1)
	go rw.loop()
	return rw
}

func (rw *renewer) loop() {
	defer rw.wg.Done()
	timer := time.NewTimer(rw.interval())
	defer timer.Stop()
	for {
		select {
		case <-rw.stop:
			return
		case <-timer.C:
			rw.renewAll()
			timer.Reset(rw.interval())
		}
	}
}

func (rw *renewer) stopAndWait() {
	close(rw.stop)
	rw.wg.Wait()
}

// renewAll 为在册的每个凭证续租一次。
//
//   - 后端明确返回“凭证不存在”（renewed=false）：凭证已丢失（通常是上个
//     周期后端不可用超过 TTL，占位被惰性回收）。此时本实例的名额确实已
//     不再被后端计入，绝不能继续当作仍占位使用——只记录事件并回调；请求
//     本身不被强杀（后续 Release 为幂等空操作），日志/指标可解释、可告警。
//   - 网络/调用错误：记一次失败指标，下个周期立即重试；只要在一个 TTL
//     内恢复就不会丢约。
//   - 续租成功但凭证恰好同时被 Release 摘除：立即做一次补偿 Release，
//     杜绝“续租与释放交错导致凭证复活”。
func (rw *renewer) renewAll() {
	curTTL := rw.ttl()
	if curTTL <= 0 {
		return // 无 TTL 占位不需要续租
	}
	holds := rw.reg.snapshot()
	items := make([]store.RenewItem, 0, len(holds))
	holdByToken := make(map[string]*leaseHold, len(holds))
	for _, h := range holds {
		if h.lost || h.ttl <= 0 || len(h.keys) == 0 {
			continue
		}
		items = append(items, store.RenewItem{Token: h.token, Keys: h.keys, TTL: curTTL})
		holdByToken[h.token] = h
	}
	if len(items) == 0 {
		return
	}
	timeout := rw.interval() * 2
	if timeout <= 0 || timeout > maxRenewCallTimeout {
		timeout = maxRenewCallTimeout
	}
	// 批量续租：内存实现在一把锁内原子完成；Redis 实现用一条 Lua 脚本
	// 原子完成。数千长连接也只有一次续租往返/一轮，单轮耗时不随 TTL 缩短
	// 而变成 O(n) 次往返。
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	missing, err := rw.st.RenewAll(ctx, items)
	cancel()
	if err != nil {
		rw.logger.Printf("quota: batch lease renew failed count=%d err=%v（下个周期重试）", len(items), err)
		if rw.metrics != nil {
			rw.metrics.IncRenewFailed()
		}
		return
	}
	missingSet := make(map[string]struct{}, len(missing))
	for _, tok := range missing {
		missingSet[tok] = struct{}{}
	}
	// 竞争复核：批量续租成功但凭证恰好同时被 Release 摘除的，立即补偿
	// Release，杜绝“续租与释放交错导致凭证复活”。
	for _, h := range holdByToken {
		if _, lostNow := missingSet[h.token]; lostNow {
			if rw.reg.markLost(h.token) {
				rw.logger.Printf("quota: lease lost in backend tenant=%s api=%s token=%s ttl=%s（实例曾失联超过租约 TTL）",
					h.tenant, h.api, h.token, curTTL)
				if rw.metrics != nil {
					rw.metrics.IncLeaseLost()
				}
				if rw.onLost != nil {
					rw.onLost(h)
				}
			}
			continue
		}
		if !rw.reg.has(h.token) {
			rctx, rcancel := context.WithTimeout(context.Background(), timeout)
			if err := rw.st.Release(rctx, h.keys, h.token); err != nil {
				rw.logger.Printf("quota: compensating release after race tenant=%s token=%s err=%v",
					h.tenant, h.token, err)
			}
			rcancel()
		}
	}
}
