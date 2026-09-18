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

	releaseTimeout = 2 * time.Second // Release 访问后端的最长时间
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
	// QueueTimeout 限制共享容量竞争时的公平排队时间；<=0 使用默认值。
	QueueTimeout time.Duration
	Metrics      *metrics.Recorder
	Logger       Logger
	Now          func() time.Time
}

// Manager 组合存储、配置与公平调度，对外提供配额判定。
type Manager struct {
	st           store.Store
	cfg          atomic.Pointer[Config]
	sched        *fairScheduler
	metrics      *metrics.Recorder
	logger       Logger
	now          func() time.Time
	instance     string
	concTTL      time.Duration
	queueTimeout time.Duration
	tokenSeq     atomic.Uint64
	closeOnce    sync.Once
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
	lg := opts.Logger
	if lg == nil {
		lg = discardLogger{}
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	m := &Manager{
		st:           opts.Store,
		sched:        newFairScheduler(),
		metrics:      opts.Metrics,
		logger:       lg,
		now:          now,
		instance:     id,
		concTTL:      ttl,
		queueTimeout: qt,
	}
	m.cfg.Store(cfg)
	if m.metrics != nil && cfg != nil {
		m.metrics.SetConfigVersion(cfg.Version)
	}
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
	releaseKeys := []string{req.ConcKey}
	if sharedKey != "" {
		releaseKeys = append(releaseKeys, sharedKey)
	}

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
		return m.degrade(cfg, tenant, api, backendCause)
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
	var once sync.Once
	release := func() {
		once.Do(func() {
			// 独立 context：请求结束（含客户端中断）也必须尽力释放，
			// 防止长连接异常路径造成并发占用泄漏。
			rctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
			defer cancel()
			if err := m.st.Release(rctx, releaseKeys, token); err != nil {
				m.logger.Printf("quota: release failed tenant=%s api=%s token=%s err=%v (ttl=%s 兜底回收)",
					tenant, api, token, err, m.concTTL)
			}
			if sharedKey != "" {
				m.sched.release(tenant)
			}
			if m.metrics != nil {
				m.metrics.DecInflight(tenant, api)
			}
		})
	}
	if m.metrics != nil {
		m.metrics.IncInflight(tenant, api)
	}
	return release, nil
}

// degrade 按配置处理后端不可用：Deny 返回可解释错误；Allow 放行并记录指标。
func (m *Manager) degrade(cfg *Config, tenant, api string, cause error) (ReleaseFunc, error) {
	m.logger.Printf("quota: backend unavailable tenant=%s api=%s policy=%s err=%v",
		tenant, api, cfg.Degrade, cause)
	if m.metrics != nil {
		m.metrics.SetStoreHealthy(false)
		m.metrics.IncDegraded(tenant, api, cfg.Degrade.String())
	}
	if cfg.Degrade == DegradeAllow {
		// fail-open：放行且不占用后端并发额度，释放为 no-op。
		return noopRelease, nil
	}
	m.recordReject(tenant, api, string(ErrCodeDegradedDeny))
	return nil, newDegradedDenyError(tenant, api)
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

// Close 关闭管理器与底层存储。
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() { err = m.st.Close() })
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
