package quota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lechandonga/quota-gateway/metrics"
	"github.com/lechandonga/quota-gateway/store"
)

const (
	// DefaultConcurrencyTTL 是并发占位的默认 TTL（实例崩溃后的兜底回收时间）。
	DefaultConcurrencyTTL = 30 * time.Second
	// DefaultQueueTimeout 是容量竞争时公平排队的最长等待。
	DefaultQueueTimeout = 1 * time.Second
	// DefaultDegradeMaxConcurrent 是降级放行期间、规则未配置并发上限时
	// 每个 (租户, 接口) 的本地在途硬上限（防止 fail-open 拖垮实例）。
	DefaultDegradeMaxConcurrent = 1024

	releaseTimeout = 2 * time.Second // Release 访问后端的最长时间

	// minRenewInterval / minReapInterval 防止后台循环周期被配置得过密。
	minRenewInterval = 10 * time.Millisecond
	minReapInterval  = 10 * time.Millisecond
)

// 内部判定哨兵错误：把“原子判定的终止原因”从调度回调传回 Acquire。
var (
	errRejRate    = errors.New("rate rejected")
	errRejConc    = errors.New("concurrency rejected")
	errRejBackend = errors.New("backend error")
)

// Logger 是最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Options 用于构造 Manager。
type Options struct {
	Store store.Store
	// Config 为 nil 表示“暂不启用配额”，之后可通过 UpdateConfig 热更新。
	Config         *Config
	InstanceID     string
	ConcurrencyTTL time.Duration
	// RenewInterval 是在途租约的续租周期；<=0 时取 ConcurrencyTTL/3。
	// 只要请求在途且实例存活，名额就会被持续续租，不会被提前回收。
	RenewInterval time.Duration
	// ReapInterval 是“调用方 ctx 已取消但尚未释放”的租约回收周期；
	// <=0 时取 100ms。与连接数无关，整个 Manager 仅一个后台循环。
	ReapInterval time.Duration
	// QueueTimeout 限制共享容量竞争时的公平排队时间；<=0 使用默认值。
	QueueTimeout time.Duration
	// DegradeMaxConcurrent 是降级放行期间、规则未配置 MaxConcurrent 时
	// 每个 (租户, 接口) 的本地在途硬上限；<=0 使用默认值。
	// 规则已配置 MaxConcurrent 时，降级放行始终遵守该硬上限。
	DegradeMaxConcurrent int64
	Metrics              *metrics.Recorder
	Logger               Logger
	Now                  func() time.Time
}

// Manager 组合存储、配置与公平调度，对外提供配额判定。
type Manager struct {
	st            store.Store
	cfg           atomic.Pointer[Config]
	sched         *fairScheduler
	metrics       *metrics.Recorder
	logger        Logger
	now           func() time.Time
	instance      string
	concTTL       time.Duration
	renewInterval time.Duration
	reapInterval  time.Duration
	queueTimeout  time.Duration
	tokenSeq      atomic.Uint64
	closeOnce     sync.Once

	leases   *leaseRegistry
	loopStop chan struct{}
	loopWg   sync.WaitGroup

	reapMu      sync.Mutex
	reapRunning bool

	// 降级放行的本地兜底：后端不可用时按 (租户, 接口) 计数的本地在途，
	// 受规则硬上限约束；恢复后从统一计账中扣除，避免额度放大。
	degMu       sync.Mutex
	degInflight map[string]int64
	degradeMax  int64

	// localInflight 是本实例按 (租户, 接口) 计数的“全部”在途请求
	// （后端占位 + 降级放行），是降级放行期间本地硬上限的判定依据：
	// 保证故障前已在途的请求与放行期间新进入的请求之和不超过配置上限。
	localMu       sync.Mutex
	localInflight map[string]int64

	// pendingReclaim 记录每个后端占位键上“名额已丢失、正在按上限补占”
	// 的租约数。补占完成前把这些预留从本实例的新获取上限中扣除，
	// 防止本实例的新请求先占掉等待补回的空位而导致本实例超额。
	pendingMu      sync.Mutex
	pendingReclaim map[string]int64
}

// NewManager 创建管理器并校验初始配置。
func NewManager(opts Options) (*Manager, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("quota: Options.Store is required")
	}
	cfg := opts.Config
	if cfg != nil {
		if err := Validate(cfg); err != nil {
			return nil, err
		}
		cfg = cfg.normalize()
	}
	id := opts.InstanceID
	if id == "" {
		id = defaultInstanceID()
	}
	ttl := opts.ConcurrencyTTL
	if ttl <= 0 {
		ttl = DefaultConcurrencyTTL
	}
	qt := opts.QueueTimeout
	if qt <= 0 {
		qt = DefaultQueueTimeout
	}
	renew := opts.RenewInterval
	if renew <= 0 {
		renew = ttl / 3
	}
	if renew < minRenewInterval {
		renew = minRenewInterval
	}
	reap := opts.ReapInterval
	if reap <= 0 {
		reap = 100 * time.Millisecond
	}
	if reap < minReapInterval {
		reap = minReapInterval
	}
	degMax := opts.DegradeMaxConcurrent
	if degMax <= 0 {
		degMax = DefaultDegradeMaxConcurrent
	}
	lg := opts.Logger
	if lg == nil {
		lg = discardLogger{}
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	m := &Manager{
		st:             opts.Store,
		sched:          newFairScheduler(),
		metrics:        opts.Metrics,
		logger:         lg,
		now:            now,
		instance:       id,
		concTTL:        ttl,
		renewInterval:  renew,
		reapInterval:   reap,
		queueTimeout:   qt,
		leases:         newLeaseRegistry(),
		loopStop:       make(chan struct{}),
		degInflight:    make(map[string]int64),
		degradeMax:     degMax,
		localInflight:  make(map[string]int64),
		pendingReclaim: make(map[string]int64),
	}
	m.cfg.Store(cfg)
	if m.metrics != nil && cfg != nil {
		m.metrics.SetConfigVersion(cfg.Version)
	}
	m.loopWg.Add(1)
	go m.leaseLoop()
	return m, nil
}

// Acquire 尝试获取配额，成功返回幂等的 ReleaseFunc。
//
// 语义：
//   - 未配置规则的租户直接放行；
//   - 速率/租户并发硬上限超限：立即返回可区分的 *Error（带 RetryAfter），
//     不会误伤未超限请求；
//   - 共享容量池竞争：成为公平队首后原子判定，未抢到容量则在队列内等待
//     （由 Release 广播唤醒重试），按租户 Weight 加权排序；受 ctx 与
//     QueueTimeout 双重约束，队列超时按并发超限拒绝；
//   - 后端不可用：按 Degrade 策略 fail-closed 拒绝，或 fail-open 本地
//     兜底放行（仍受该租户并发硬上限约束，见 degradeAllow）；
//   - ctx 取消（如客户端断连）不产生任何占位。
//
// 成功获取的名额由后台租约循环续租：请求在途多久都不会被提前回收；
// 持有实例崩溃时名额在 ConcurrencyTTL 内有界回收。
//
// 返回的 ReleaseFunc 幂等：正常结束、超时、客户端中断（后台回收）、
// 重复调用、乱序调用都安全；存储异常路径另有占位 TTL 兜底。
func (m *Manager) Acquire(ctx context.Context, tenant, api string) (ReleaseFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg := m.cfg.Load()
	if cfg == nil {
		return noopRelease, nil
	}
	rule, ok := cfg.RuleFor(tenant, api)
	if !ok {
		return noopRelease, nil
	}
	if m.metrics != nil {
		m.metrics.IncRequests(tenant, api)
	}

	token := m.newToken()
	sharedKey := ""
	if cfg.GlobalMaxConcurrent > 0 {
		sharedKey = "sc:global"
	}
	req := store.AcquireRequest{
		RateKey:       "rt:" + tenant + ":" + api,
		ConcKey:       "cc:" + tenant + ":" + api,
		SharedKey:     sharedKey,
		Rate:          rule.Rate,
		Burst:         rule.Burst,
		MaxConcurrent: rule.MaxConcurrent,
		MaxShared:     cfg.GlobalMaxConcurrent,
		Token:         token,
		TTL:           m.concTTL,
	}
	// heldKeys 是本次获取实际会写入后端的占位键：
	// MaxConcurrent<=0 的规则不写租户键；共享池未启用/不限制时不写共享键。
	// 续租分组与释放都只针对实际持有的键。
	heldKeys := make([]string, 0, 2)
	if req.MaxConcurrent > 0 {
		heldKeys = append(heldKeys, req.ConcKey)
	}
	if sharedKey != "" && req.MaxShared > 0 {
		heldKeys = append(heldKeys, sharedKey)
	}

	// 降级恢复过渡：本地降级在途不写入后端，这里从租户硬上限中扣除，
	// 使“后端占用 + 本地降级在途”之和不超过配置上限（不放大、不重复计数）；
	// 本地在途随请求结束自然归零后自动恢复全额。
	if req.MaxConcurrent > 0 {
		if n := m.degradedLocal(tenant, api); n > 0 {
			if n >= req.MaxConcurrent {
				m.recordReject(tenant, api, string(ErrCodeConcurrencyLimited))
				return nil, newConcurrencyLimitedError(tenant, api, m.reapInterval)
			}
			req.MaxConcurrent -= n
		}
		// 注意：本实例的补占预留（pendingReclaim）不在此扣除——那些丢失
		// 租约仍在本实例统一本地在途闸口内计数（本地新请求已被闸口挡住），
		// 再扣一次会把额度预留两遍、误拒正常请求。
	}
	// 共享池是全局资源：本实例丢失待补占的共享名额必须从后端共享上限中
	// 扣除，防止本实例新请求与补占争用全局空位而放大全局在途。
	sharedReservedFull := false
	if sharedKey != "" && req.MaxShared > 0 {
		if n := m.pendingForKey(sharedKey); n > 0 {
			if n >= req.MaxShared {
				sharedReservedFull = true
			} else {
				req.MaxShared -= n
			}
		}
	}

	// 原子判定在“成为公平队首”时执行；未配置共享池时只判定一次。
	var res store.Result
	var backendCause error
	tryOnce := func() (admitOutcome, error) {
		if sharedReservedFull {
			res = store.Result{Status: store.StatusSharedLimited, RetryAfter: m.renewInterval}
			return outcomeContended, nil
		}
		r, err := m.st.TryAcquire(ctx, req)
		if err != nil {
			backendCause = err
			return outcomeRejected, errRejBackend
		}
		switch r.Status {
		case store.StatusAcquired:
			res = r
			return outcomeAdmitted, nil
		case store.StatusSharedLimited:
			res = r
			return outcomeContended, nil
		case store.StatusRateLimited:
			res = r
			return outcomeRejected, errRejRate
		default:
			res = r
			return outcomeRejected, errRejConc
		}
	}

	var admErr error
	if sharedKey == "" {
		if _, err := tryOnce(); err != nil {
			admErr = err
		}
	} else {
		qctx, cancel := context.WithTimeout(ctx, m.queueTimeout)
		admErr = m.sched.acquire(qctx, tenant, rule.Weight, tryOnce)
		cancel()
	}

	switch {
	case admErr == nil:
		// 成功，构造幂等释放函数。
	case errors.Is(admErr, errRejRate):
		m.recordReject(tenant, api, string(ErrCodeRateLimited))
		return nil, newRateLimitedError(tenant, api, res.RetryAfter)
	case errors.Is(admErr, errRejConc):
		m.recordReject(tenant, api, string(ErrCodeConcurrencyLimited))
		return nil, newConcurrencyLimitedError(tenant, api, res.RetryAfter)
	case errors.Is(admErr, errRejBackend):
		return m.degrade(ctx, cfg, tenant, api, rule, backendCause)
	case errors.Is(admErr, context.DeadlineExceeded) && ctx.Err() == nil:
		// 公平排队超过 QueueTimeout（调用方自身 ctx 未到期）。
		m.recordReject(tenant, api, string(ErrCodeConcurrencyLimited))
		return nil, newConcurrencyLimitedError(tenant, api, m.queueTimeout)
	default:
		// 调用方 ctx 取消/超时（含客户端主动断连）：未占位，直接返回。
		return nil, admErr
	}

	if m.metrics != nil {
		m.metrics.IncAllowed(tenant, api)
	}
	// 提交点：后端占位已拿到，登记统一本地在途（与降级放行共用同一闸口，
	// 保证任何后端状态下本实例同时运行的请求数不超过本地上限）。
	localLimit := rule.MaxConcurrent
	if localLimit <= 0 {
		localLimit = m.degradeMax
	}
	if !m.reserveLocalSlot(tenant, api, localLimit) {
		// 理论上仅在降级在途与后端判定的极小交错下可达：立即归还刚占的
		// 后端名额（幂等），按并发超限拒绝，绝不带着双份占位返回。
		rctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		_ = m.st.Release(rctx, heldKeys, token)
		cancel()
		if sharedKey != "" {
			m.sched.release(tenant)
		}
		m.recordReject(tenant, api, string(ErrCodeConcurrencyLimited))
		return nil, newConcurrencyLimitedError(tenant, api, m.reapInterval)
	}
	// 登记在途租约：后台循环负责续租（长连接名额不被提前回收）与
	// ctx 取消回收（客户端断连但 handler 未返回时名额有界释放）。
	entry := &leaseEntry{token: token, keys: heldKeys, tenant: tenant, api: api, ctx: ctx}
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.leases.remove(token)
			// 与后台“丢失补占”互斥：补占进行中时不得删除后端占位，
			// 否则会把刚补回的名额再次弄丢（提前归还）。补占流程本身
			// 会检查租约是否仍登记，已结束的租约不会被补占。
			entry.mu.Lock()
			skipBackendRelease := entry.lost
			entry.mu.Unlock()
			switch {
			case !skipBackendRelease:
				// 独立 context：请求结束（含客户端中断）也必须尽力释放，
				// 防止长连接异常路径造成并发占用泄漏。
				rctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
				if err := m.st.Release(rctx, heldKeys, token); err != nil {
					m.logger.Printf("quota: release failed tenant=%s api=%s token=%s err=%v (ttl=%s 兜底回收)",
						tenant, api, token, err, m.concTTL)
				}
				cancel()
			case len(heldKeys) > 0:
				// 名额丢失、补占尚未完成（上限满/后端暂不可达）：后端没有
				// 本 token 可删；释放本租约在各持有键上的补占预留，
				// 让额度立即可以被其他请求使用（后台循环不会再重试本租约，
				// 因为登记表中已移除）。
				m.releasePending(heldKeys)
			}
			if sharedKey != "" {
				m.sched.release(tenant)
			}
			m.decLocalInflight(tenant, api)
			if m.metrics != nil {
				m.metrics.DecInflight(tenant, api)
			}
		})
	}
	entry.release = release
	m.leases.add(entry)
	if m.metrics != nil {
		m.metrics.IncInflight(tenant, api)
	}
	return release, nil
}

// degrade 按配置处理后端不可用：Deny 返回可解释错误；Allow 走本地兜底
// 放行（统一本地在途闸口保证不超额）。
func (m *Manager) degrade(ctx context.Context, cfg *Config, tenant, api string, rule Rule, cause error) (ReleaseFunc, error) {
	m.logger.Printf("quota: backend unavailable tenant=%s api=%s policy=%s err=%v",
		tenant, api, cfg.Degrade, cause)
	if m.metrics != nil {
		m.metrics.SetStoreHealthy(false)
	}
	if cfg.Degrade == DegradeAllow {
		return m.degradeAllow(ctx, tenant, api, rule)
	}
	if m.metrics != nil {
		m.metrics.IncDegraded(tenant, api, DegradeDeny.String())
	}
	m.recordReject(tenant, api, string(ErrCodeDegradedDeny))
	return nil, newDegradedDenyError(tenant, api)
}

// degradeAllow 本地兜底放行：不占用后端额度。本实例上该 (租户, 接口)
// 的并发硬上限由 Acquire 入口的“统一本地在途闸口”保证——故障前已在途的
// 后端租约、降级期间新放行请求、后端恢复后的正常请求共用同一计数与预留，
// 任何后端状态下同时运行的请求数都不超过上限（规则未配置时用
// DegradeMaxConcurrent）。
//
// 本函数只负责降级维度的登记/回收与可观测；本地在途名额已由调用方预留，
// 受理时 commit（请求结束随 release 回收），不再自行计数。
//
// 恢复语义：降级期间的在途请求始终只计在本地，从不写入后端，因此后端
// 恢复后不会被重复计数；同时正常获取路径会把本地降级在途数从租户硬
// 上限中扣除，使“后端占用 + 本地降级在途”之和始终不超过配置上限
// （不放大额度），本地在途随请求结束自然归零后恢复全额。
func (m *Manager) degradeAllow(ctx context.Context, tenant, api string, rule Rule) (ReleaseFunc, error) {
	limit := rule.MaxConcurrent
	if limit <= 0 {
		limit = m.degradeMax
	}
	// 统一本地在途闸口（与后端获取成功路径同一个）：故障前在途、放行期
	// 新请求、恢复后正常请求共用同一计数，本实例同时运行数绝不超过上限。
	if !m.reserveLocalSlot(tenant, api, limit) {
		m.logger.Printf("quota: degrade allow but local cap reached tenant=%s api=%s inflight=%d degraded=%d cap=%d",
			tenant, api, m.localInflightTotal(tenant, api), m.degradedLocal(tenant, api), limit)
		if m.metrics != nil {
			m.metrics.IncDegraded(tenant, api, "local_limited")
		}
		m.recordReject(tenant, api, string(ErrCodeConcurrencyLimited))
		return nil, newConcurrencyLimitedError(tenant, api, m.reapInterval)
	}
	key := tenant + "\x00" + api
	m.degMu.Lock()
	m.degInflight[key]++
	m.degMu.Unlock()

	if m.metrics != nil {
		m.metrics.IncDegraded(tenant, api, DegradeAllow.String())
		m.metrics.IncDegradedInflight(tenant, api)
		m.metrics.IncInflight(tenant, api)
		m.metrics.IncAllowed(tenant, api)
	}
	// 登记租约（无后端键）：客户端断连但 handler 未返回时由后台循环回收。
	token := m.newToken()
	entry := &leaseEntry{token: token, tenant: tenant, api: api, ctx: ctx}
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.leases.remove(token)
			m.degMu.Lock()
			if m.degInflight[key] > 0 {
				m.degInflight[key]--
			}
			if m.degInflight[key] == 0 {
				delete(m.degInflight, key)
			}
			m.degMu.Unlock()
			// 统一本地在途与指标在 release 中各回收一次（与正常路径相同）。
			m.decLocalInflight(tenant, api)
			if m.metrics != nil {
				m.metrics.DecDegradedInflight(tenant, api)
				m.metrics.DecInflight(tenant, api)
			}
		})
	}
	entry.release = release
	m.leases.add(entry)
	return release, nil
}

// degradedLocal 返回该 (租户, 接口) 当前降级放行的本地在途数。
func (m *Manager) degradedLocal(tenant, api string) int64 {
	m.degMu.Lock()
	defer m.degMu.Unlock()
	return m.degInflight[tenant+"\x00"+api]
}

// localKey 是本实例在途计数的键。
func localKey(tenant, api string) string { return tenant + "\x00" + api }

// incLocalInflight / decLocalInflight 维护本实例 (租户, 接口) 的全部在途
// 计数（后端占位 + 降级放行）。降级放行期间以它作为本地硬上限判定依据，
// 保证故障前在途与放行期新进入的请求之和不超过配置上限。
func (m *Manager) incLocalInflight(tenant, api string) {
	k := localKey(tenant, api)
	m.localMu.Lock()
	m.localInflight[k]++
	m.localMu.Unlock()
}

func (m *Manager) decLocalInflight(tenant, api string) {
	k := localKey(tenant, api)
	m.localMu.Lock()
	if m.localInflight[k] > 0 {
		m.localInflight[k]--
	}
	if m.localInflight[k] == 0 {
		delete(m.localInflight, k)
	}
	m.localMu.Unlock()
}

// localInflightTotal 返回本实例该 (租户, 接口) 的全部在途数。
func (m *Manager) localInflightTotal(tenant, api string) int64 {
	m.localMu.Lock()
	defer m.localMu.Unlock()
	return m.localInflight[localKey(tenant, api)]
}

// reserveLocalSlot 在本实例为一个“已确定受理”的请求原子预留 1 个统一在途
// 名额（后端获取成功、降级放行两个提交点共用）。上限取规则 MaxConcurrent，
// 未配置时取 DegradeMaxConcurrent；已满返回 false（调用方按并发超限拒绝，
// 且不得保留任何后端占位之外的本地状态）。
func (m *Manager) reserveLocalSlot(tenant, api string, limit int64) bool {
	if limit <= 0 {
		return true
	}
	k := localKey(tenant, api)
	m.localMu.Lock()
	if m.localInflight[k] >= limit {
		m.localMu.Unlock()
		return false
	}
	m.localInflight[k]++
	m.localMu.Unlock()
	return true
}

// pendingForKey 返回某个后端占位键上等待补占的丢失租约数。
func (m *Manager) pendingForKey(key string) int64 {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	return m.pendingReclaim[key]
}

// releasePending 在补占未完成的租约结束时释放其持有键上的预留。
func (m *Manager) releasePending(keys []string) {
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

// UpdateConfig 热更新配置：先校验，非法或不兼容时保持旧配置不变。
func (m *Manager) UpdateConfig(c *Config) error {
	if c == nil {
		return newConfigError("", "config is nil")
	}
	if err := Validate(c); err != nil {
		m.logger.Printf("quota: reject invalid config version=%q err=%v", c.Version, err)
		if m.metrics != nil {
			m.metrics.IncConfigRejected(c.Version)
		}
		return err
	}
	old := m.cfg.Load()
	next := c.normalize()
	// 版本演进：允许同主版本内的升/降级（新旧实例共存）；
	// 主版本不兼容或低于最低支持版本的配置已在 Validate 中拒绝。
	m.cfg.Store(next)
	if m.metrics != nil {
		m.metrics.SetConfigVersion(next.Version)
	}
	oldV := ""
	if old != nil {
		oldV = old.Version
	}
	m.logger.Printf("quota: config activated old=%s new=%s rules=%d shared=%d",
		oldV, next.Version, len(next.Rules), next.GlobalMaxConcurrent)
	return nil
}

// ConfigVersion 返回当前生效配置版本；未加载配置时返回空串。
func (m *Manager) ConfigVersion() string {
	cfg := m.cfg.Load()
	if cfg == nil {
		return ""
	}
	return cfg.Version
}

// SetStoreHealthy 供外部健康探测更新后端健康指标。
func (m *Manager) SetStoreHealthy(healthy bool) {
	if m.metrics != nil {
		m.metrics.SetStoreHealthy(healthy)
	}
}

// Close 停止后台租约循环并关闭底层存储。
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		close(m.loopStop)
		m.loopWg.Wait()
		err = m.st.Close()
	})
	return err
}

func (m *Manager) newToken() string {
	seq := m.tokenSeq.Add(1)
	return fmt.Sprintf("%s-%d", m.instance, seq)
}

func (m *Manager) recordReject(tenant, api, reason string) {
	if m.metrics != nil {
		m.metrics.IncRejected(tenant, api, reason)
	}
}

func noopRelease() {}

// defaultInstanceID 生成尽量全局唯一的实例标识（主机名+pid+随机后缀），
// 多实例并发占位时 token 不会碰撞。
func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "inst"
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}
