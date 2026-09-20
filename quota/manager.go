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
	// DefaultConcurrencyTTL 是并发占位租约的默认 TTL（实例崩溃后的兜底回收时间）。
	DefaultConcurrencyTTL = 30 * time.Second
	// DefaultQueueTimeout 是容量竞争时公平排队的最长等待。
	DefaultQueueTimeout = 1 * time.Second

	releaseTimeout = 2 * time.Second // Release 访问后端的最长时间
	minRenewTick   = 100 * time.Millisecond
	maxRenewTick   = 10 * time.Second
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
	Config *Config
	// InstanceID 标识本实例；空则自动生成（主机名+pid+随机后缀）。
	InstanceID string
	// ConcurrencyTTL 是并发占位租约 TTL；<=0 使用默认值（30s）。
	// 配置文件中的 concurrencyTTLSeconds 优先级更高。
	ConcurrencyTTL time.Duration
	// QueueTimeout 限制共享容量竞争时的公平排队时间；<=0 使用默认值。
	QueueTimeout time.Duration
	// RenewInterval 是长请求占位的续租周期；<=0 时取 TTL/3（封顶 10s）。
	// 正常运行的实例持续续租，因此长请求名额不会被 TTL 提前回收。
	RenewInterval time.Duration
	Metrics       *metrics.Recorder
	Logger        Logger
	Now           func() time.Time
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
	queueTimeout  time.Duration
	tokenSeq      atomic.Uint64
	leases        *leaseRegistry
	gate          *localGate
	renewer       *renewer
	closeOnce     sync.Once
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
	ri := opts.RenewInterval
	if ri <= 0 {
		ri = ttl / 3
		if ri < minRenewTick {
			ri = minRenewTick
		}
		if ri > maxRenewTick {
			ri = maxRenewTick
		}
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
		st:            opts.Store,
		sched:         newFairScheduler(),
		metrics:       opts.Metrics,
		logger:        lg,
		now:           now,
		instance:      id,
		concTTL:       ttl,
		renewInterval: ri,
		queueTimeout:  qt,
		leases:        newLeaseRegistry(),
		gate:          newLocalGate(),
	}
	m.cfg.Store(cfg)
	if m.metrics != nil && cfg != nil {
		m.metrics.SetConfigVersion(cfg.Version)
	}
	// 单 goroutine 续租：与连接数无关，数千长连接也只有一个定时器。
	// 续租周期默认随生效 TTL 动态推导（TTL/3），热更新把 TTL 调小也能
	// 及时续租；opts.RenewInterval 仅作为测试/调优的固定覆盖。
	var rm renewMetrics
	if m.metrics != nil {
		rm = m.metrics
	}
	m.renewer = startRenewer(m.leases, m.st,
		func() time.Duration { return m.effectiveTTL() },
		m.renewIntervalFn,
		m.logger, rm, m.onLeaseLost)
	return m, nil
}

// renewIntervalFn 返回当前应使用的续租周期：显式覆盖优先，否则按生效
// TTL 的 1/3 动态计算（钳制在 [100ms, 10s]），保证每个 TTL 周期内至少
// 续租两次，且不会因 TTL 过大产生频繁的后端调用。
func (m *Manager) renewIntervalFn() time.Duration {
	if m.renewInterval > 0 {
		return m.renewInterval
	}
	d := m.effectiveTTL() / 3
	if d < minRenewTick {
		d = minRenewTick
	}
	if d > maxRenewTick {
		d = maxRenewTick
	}
	return d
}

// effectiveTTL 返回当前配置下的租约 TTL：配置项优先，其次构造参数/默认值。
func (m *Manager) effectiveTTL() time.Duration {
	if cfg := m.cfg.Load(); cfg != nil && cfg.ConcurrencyTTLSeconds > 0 {
		return time.Duration(cfg.ConcurrencyTTLSeconds) * time.Second
	}
	return m.concTTL
}

// onLeaseLost 在凭证被确认于后端丢失时回调。
//
// 这里故意*不*归还本地闸门名额：凭证在后端丢失通常意味着本实例曾失联超过
// 一个 TTL（网络分区/长时间 STW 等），该名额可能已被后端回收并分配给了
// 其他实例。为了在分裂窗口内尽量不放大额度，本地仍按“占用”计数，直到这
// 个请求真正结束（finishHold）才归还——采取保守的少放行而非多放行。
func (m *Manager) onLeaseLost(h *leaseHold) {
	m.logger.Printf("quota: keep local slot counted until request ends tenant=%s api=%s token=%s",
		h.tenant, h.api, h.token)
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
//   - 后端不可用：按 Degrade 策略 fail-open 放行或 fail-closed 拒绝；
//   - ctx 取消（如客户端断连）不产生任何占位。
//
// 返回的 ReleaseFunc 幂等：正常结束、超时、客户端中断（defer release）、
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
	effTTL := m.effectiveTTL()
	req := store.AcquireRequest{
		RateKey:       "rt:" + tenant + ":" + api,
		ConcKey:       "cc:" + tenant + ":" + api,
		SharedKey:     sharedKey,
		Rate:          rule.Rate,
		Burst:         rule.Burst,
		MaxConcurrent: rule.MaxConcurrent,
		MaxShared:     cfg.GlobalMaxConcurrent,
		Token:         token,
		TTL:           effTTL,
	}
	// 仅当该维度实际启用了并发上限，后端才会写入占位；续租/释放只携带
	// 真正持有占位的键，避免对“不限并发”的键误判凭证丢失。
	var holdKeys []string
	if rule.MaxConcurrent > 0 {
		holdKeys = append(holdKeys, req.ConcKey)
	}
	if sharedKey != "" && cfg.GlobalMaxConcurrent > 0 {
		holdKeys = append(holdKeys, sharedKey)
	}
	releaseKeys := holdKeys
	localKey := tenant + "\x00" + api

	// 原子判定在“成为公平队首”时执行；未配置共享池时只判定一次。
	var res store.Result
	var backendCause error
	tryOnce := func() (admitOutcome, error) {
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
		return m.degrade(cfg, tenant, api, localKey, rule.MaxConcurrent, backendCause)
	case errors.Is(admErr, context.DeadlineExceeded) && ctx.Err() == nil:
		// 公平排队超过 QueueTimeout（调用方自身 ctx 未到期）。
		m.recordReject(tenant, api, string(ErrCodeConcurrencyLimited))
		return nil, newConcurrencyLimitedError(tenant, api, m.queueTimeout)
	default:
		// 调用方 ctx 取消/超时（含客户端主动断连）：未占位，直接返回。
		return nil, admErr
	}

	// 后端已占位；再尝试本地闸门。本地名额仅可能被“降级期间遗留的在途
	// 请求”占用：正常稳态下后端硬上限已保证本实例在途 <= maxConcurrent，
	// 闸门恒有余量。闸门满只发生在后端刚恢复、降级在途尚未排空时，拒绝
	// 新请求可保证本机在途绝不超过上限，且不向已恢复的后端放大额度。
	if !m.gate.tryAcquire(localKey, rule.MaxConcurrent) {
		rctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		if err := m.st.Release(rctx, releaseKeys, token); err != nil {
			m.logger.Printf("quota: rollback release failed tenant=%s token=%s err=%v (ttl=%s 兜底)",
				tenant, token, err, effTTL)
		}
		cancel()
		if sharedKey != "" {
			m.sched.release(tenant)
		}
		m.recordReject(tenant, api, string(ErrCodeConcurrencyLimited))
		return nil, newConcurrencyLimitedError(tenant, api, time.Second)
	}

	hold := &leaseHold{
		token:    token,
		keys:     releaseKeys,
		tenant:   tenant,
		api:      api,
		ttl:      effTTL,
		shared:   sharedKey != "",
		localKey: localKey,
	}
	m.leases.add(hold)

	if m.metrics != nil {
		m.metrics.IncAllowed(tenant, api)
		m.metrics.IncInflight(tenant, api)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.finishHold(hold)
		})
	}
	return release, nil
}

// finishHold 完成一次占用的全部清理，顺序经过精心设计以杜绝交错问题：
//
//  1. 先从续租注册表摘除——此后续租 goroutine 不会再碰这个凭证；
//  2. 再调用后端 Release（独立 context：客户端断连也要尽力释放）；
//  3. 归还本地闸门名额；
//  4. 唤醒公平队列并更新在途指标。
//
// 即使续租恰好与本函数并发：续租成功后会复核注册表并做补偿 Release，
// 因此不会出现“释放后凭证又被续租复活”。重复/乱序释放由 sync.Once 兜底。
func (m *Manager) finishHold(h *leaseHold) {
	m.leases.remove(h.token)
	if !h.lost {
		rctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		if err := m.st.Release(rctx, h.keys, h.token); err != nil {
			m.logger.Printf("quota: release failed tenant=%s api=%s token=%s err=%v (ttl=%s 兜底回收)",
				h.tenant, h.api, h.token, err, h.ttl)
		}
		cancel()
	}
	m.gate.release(h.localKey)
	if h.shared {
		m.sched.release(h.tenant)
	}
	if m.metrics != nil {
		m.metrics.DecInflight(h.tenant, h.api)
	}
}

// degrade 按配置处理后端不可用：Deny 返回可解释错误；Allow 在本地并发
// 硬上限内放行并记录指标。
//
// fail-open 时后端无法计账，由本地闸门保证同一实例同一 (租户,接口) 的
// 在途数不超过规则 maxConcurrent；这些本地占用在后端恢复后仍继续保留到
// 请求结束，恢复时新请求走后端计账，两边合计不放大额度、不重复计数。
func (m *Manager) degrade(cfg *Config, tenant, api, localKey string, maxConc int64, cause error) (ReleaseFunc, error) {
	m.logger.Printf("quota: backend unavailable tenant=%s api=%s policy=%s err=%v",
		tenant, api, cfg.Degrade, cause)
	if m.metrics != nil {
		m.metrics.SetStoreHealthy(false)
		m.metrics.IncDegraded(tenant, api, cfg.Degrade.String())
	}
	if cfg.Degrade != DegradeAllow {
		m.recordReject(tenant, api, string(ErrCodeDegradedDeny))
		return nil, newDegradedDenyError(tenant, api)
	}
	if !m.gate.tryAcquire(localKey, maxConc) {
		// 降级放行也不允许冲破本地硬上限：可解释地拒绝，避免拖垮实例。
		m.logger.Printf("quota: degrade-allow local cap reached tenant=%s api=%s max=%d",
			tenant, api, maxConc)
		m.recordReject(tenant, api, string(ErrCodeDegradedLocalLimited))
		return nil, newDegradedLocalLimitedError(tenant, api)
	}
	if m.metrics != nil {
		m.metrics.IncAllowed(tenant, api)
		m.metrics.IncInflight(tenant, api)
		m.metrics.AddDegradedLocalInflight(1)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			m.gate.release(localKey)
			if m.metrics != nil {
				m.metrics.AddDegradedLocalInflight(-1)
				m.metrics.DecInflight(tenant, api)
			}
		})
	}, nil
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

// Close 停止续租并关闭管理器与底层存储。
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		if m.renewer != nil {
			m.renewer.stopAndWait() // 先停续租，避免对已关闭的后端发起调用
		}
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
