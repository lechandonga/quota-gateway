package quota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lechandonga/quota-gateway/metrics"
	"github.com/lechandonga/quota-gateway/store"
)

// DefaultConcurrencyTTL 是并发占位的默认 TTL（兜底回收时间）。
const DefaultConcurrencyTTL = 30 * time.Second

// releaseTimeout 限制 Release 访问后端的最长时间。
const releaseTimeout = 2 * time.Second

// Logger 是最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Options 用于构造 Manager。
type Options struct {
	Store          store.Store
	Config         *Config
	InstanceID     string
	ConcurrencyTTL time.Duration
	Metrics        *metrics.Recorder
	Logger         Logger
	Now            func() time.Time
}

// Manager 组合存储、配置与公平调度，对外提供配额判定。
type Manager struct {
	st        store.Store
	cfg       atomic.Pointer[Config]
	sched     *fairScheduler
	metrics   *metrics.Recorder
	logger    Logger
	now       func() time.Time
	instance  string
	concTTL   time.Duration
	tokenSeq  atomic.Uint64
	closeOnce sync.Once
}

// NewManager 创建管理器并校验初始配置；初始配置为 nil 表示“暂不启用配额”，
// 之后可通过 UpdateConfig 热更新。
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
	lg := opts.Logger
	if lg == nil {
		lg = discardLogger{}
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	m := &Manager{
		st:       opts.Store,
		sched:    newFairScheduler(),
		metrics:  opts.Metrics,
		logger:   lg,
		now:      now,
		instance: id,
		concTTL:  ttl,
	}
	m.cfg.Store(cfg)
	if m.metrics != nil && cfg != nil {
		m.metrics.SetConfigVersion(cfg.Version)
	}
	return m, nil
}

// Acquire 以非阻塞的“短等待”方式尝试获取配额：
//   - 速率/并发确定超限时立即返回可区分的 *quota.Error（带 RetryAfter）；
//   - 容量竞争时按租户权重公平排队（受 ctx 截止时间约束）；
//   - 后端不可用时按配置的降级策略放行或拒绝；
//   - 成功时返回的 ReleaseFunc 幂等：正常结束、超时、客户端中断
//     （defer release）、重复调用、乱序调用都安全。
//
// ctx 取消（如客户端主动断开）时返回 ctx.Err()，不产生任何占位。
func (m *Manager) Acquire(ctx context.Context, tenant, api string) (ReleaseFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg := m.cfg.Load()
	if cfg == nil {
		return noopRelease, nil // 未启用配额
	}
	rule, ok := cfg.RuleFor(tenant, api)
	if !ok {
		return noopRelease, nil // 该租户未配置规则：不施加配额
	}
	if m.metrics != nil {
		m.metrics.IncRequests(tenant, api)
	}

	// 并发类规则参与加权公平排队，避免单一租户长期占满并发容量。
	if rule.MaxConcurrent > 0 {
		if err := m.sched.wait(ctx, tenant, rule.Weight); err != nil {
			return nil, err // 客户端取消或超时：未占位，直接退出队列
		}
	}

	token := m.newToken()
	rateKey := "rt:" + tenant + ":" + api
	concKey := "cc:" + tenant + ":" + api
	res, err := m.st.TryAcquire(ctx, rateKey, concKey, rule.Rate, rule.Burst, rule.MaxConcurrent, token, m.concTTL)
	if err != nil {
		if rule.MaxConcurrent > 0 {
			m.sched.release(tenant)
		}
		return m.degrade(cfg, tenant, api, err)
	}

	switch res.Status {
	case store.StatusRateLimited:
		if rule.MaxConcurrent > 0 {
			m.sched.release(tenant)
		}
		m.recordReject(tenant, api, string(ErrCodeRateLimited))
		return nil, newRateLimitedError(tenant, api, res.RetryAfter)
	case store.StatusConcurrencyLimited:
		if rule.MaxConcurrent > 0 {
			m.sched.release(tenant)
		}
		m.recordReject(tenant, api, string(ErrCodeConcurrencyLimited))
		return nil, newConcurrencyLimitedError(tenant, api, res.RetryAfter)
	}

	if m.metrics != nil {
		m.metrics.IncAllowed(tenant, api)
	}

	var once sync.Once
	var released atomic.Bool
	release := func() {
		once.Do(func() {
			// 使用独立 context：请求已结束（含客户端中断）也必须尽力释放，
			// 防止长连接异常路径造成并发占用泄漏。
			rctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
			defer cancel()
			if err := m.st.Release(rctx, concKey, token); err != nil {
				m.logger.Printf("quota: release failed tenant=%s api=%s token=%s err=%v (ttl=%s 兜底回收)",
					tenant, api, token, err, m.concTTL)
			}
			if rule.MaxConcurrent > 0 {
				m.sched.release(tenant)
			}
			if m.metrics != nil {
				m.metrics.DecInflight(tenant, api)
			}
			released.Store(true)
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
		// fail-open：放行且不占用后端并发额度，因此释放为 no-op。
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
	// 版本演进：允许同主版本内的升/降级（新旧实例共存），
	// 拒绝主版本号不兼容或低于最低支持版本的配置（Validate 已校验）。
	m.cfg.Store(next)
	if m.metrics != nil {
		m.metrics.SetConfigVersion(next.Version)
	}
	oldV := ""
	if old != nil {
		oldV = old.Version
	}
	m.logger.Printf("quota: config activated old=%s new=%s rules=%d", oldV, next.Version, len(next.Rules))
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

// defaultInstanceID 生成尽量全局唯一的实例标识（主机名+pid+随机后缀）。
func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "inst"
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}
