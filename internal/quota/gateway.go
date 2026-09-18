package quota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
)

// 默认时序参数（可被 Options/配置覆盖）。
const (
	defaultSessionTTL  = 30 * time.Second
	defaultHeartbeat   = 5 * time.Second
	defaultCallTimeout = 2 * time.Second
	defaultMaxWait     = 5 * time.Second
	defaultBackoff     = 25 * time.Millisecond
)

// Gateway 是配额准入层：速率限制 + 并发/公平调度的组合入口。
type Gateway struct {
	mgr      *Manager
	store    Store
	clock    Clock
	metrics  Metrics
	degrader *Degrader
	mode     Mode
	maxWait  time.Duration
	instance string

	closeOnce sync.Once
	closed    atomic.Bool
}

// Options 构造 Gateway 的可选项。
type Options struct {
	Clock       Clock
	Metrics     Metrics
	Store       Store // 为空时 local 模式自建 MemoryStore；distributed 模式自建 RemoteStore
	Mode        Mode
	Degrade     DegradePolicy
	BaseURL     string // distributed 模式协调服务地址
	SessionTTL  time.Duration
	Heartbeat   time.Duration
	CallTimeout time.Duration
	MaxWait     time.Duration // 公平排队最长等待
	InstanceID  string
}

// Lease 表示一次成功占有的并发名额，必须 Release。
type Lease struct {
	gateway  *Gateway
	token    LeaseToken
	reqID    RequestID
	tenant   string
	iface    string
	fallback bool // true 表示 fail-open 本地兜底名额
	relOnce  sync.Once
}

// Release 归还名额；重复调用、乱序调用、panic 后调用均安全。
func (l *Lease) Release() {
	l.relOnce.Do(func() {
		l.gateway.releaseLease(l)
	})
}

func newRequestID() RequestID {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return RequestID(hex.EncodeToString(b[:]))
}

// NewGateway 创建配额网关。初始配置必须通过校验，否则返回错误且无副作用。
func NewGateway(initial *Config, opts Options) (*Gateway, error) {
	if err := ValidateConfig(initial); err != nil {
		return nil, err
	}
	clock := opts.Clock
	if clock == nil {
		clock = SystemClock()
	}
	metrics := opts.Metrics
	mgr, err := NewManager(initial, clock, metrics)
	if err != nil {
		return nil, err
	}
	mode := opts.Mode
	if mode == "" {
		mode = initial.Mode
	}
	policy := opts.Degrade
	if policy == "" {
		policy = initial.Degrade
	}
	store := opts.Store
	if store == nil {
		switch mode {
		case ModeDistributed:
			rc, err := NewRemoteStore(context.Background(), RemoteOptions{
				BaseURL:    opts.BaseURL,
				InstanceID: opts.InstanceID,
				Clock:      clock,
				// Options 显式值优先；其次取配置文件；最后内置默认值。
				SessionTTL:  durationMillis(optOrCfg(opts.SessionTTL, initial.SessionTTLMillis), defaultSessionTTL),
				Heartbeat:   durationMillis(optOrCfg(opts.Heartbeat, initial.HeartbeatMillis), defaultHeartbeat),
				CallTimeout: durationMillis(optOrCfg(opts.CallTimeout, initial.RequestTimeoutMillis), defaultCallTimeout),
			})
			if err != nil {
				return nil, err
			}
			store = rc
		default:
			ms := NewMemoryStore(clock)
			store = ms
		}
	}
	if err := store.SyncConfig(context.Background(), initial); err != nil {
		_ = store.Close()
		return nil, err
	}
	g := &Gateway{
		mgr:      mgr,
		store:    store,
		clock:    clock,
		metrics:  metrics,
		degrader: NewDegrader(policy),
		mode:     mode,
		maxWait:  opts.MaxWait,
		instance: opts.InstanceID,
	}
	if g.maxWait <= 0 {
		g.maxWait = defaultMaxWait
	}
	return g, nil
}

func durationMillis(d, def time.Duration) int64 {
	if d <= 0 {
		d = def
	}
	return d.Milliseconds()
}

// optOrCfg 在 Options 未显式设置时回退到配置文件中的毫秒值（0 表示未配置）。
func optOrCfg(opt time.Duration, cfgMillis int64) time.Duration {
	if opt > 0 {
		return opt
	}
	if cfgMillis > 0 {
		return time.Duration(cfgMillis) * time.Millisecond
	}
	return 0
}

// Acquire 为一个请求申请配额。
// 成功返回非空 Lease（调用方必须在任何结束路径上 Release，含超时与客户端中断）；
// 失败返回 *QuotaError，携带可机读 Reason 与 RetryIn 重试提示。
// 未配置限额的租户/接口直接放行，不做任何计数，不会被误伤。
func (g *Gateway) Acquire(ctx context.Context, tenant, iface string) (*Lease, error) {
	cfg := g.mgr.Current()
	lim, limited := cfg.MatchLimit(tenant, iface)
	if !limited {
		// 未配置：放行，返回空租约（Release 是 no-op）。
		g.recordAllowed(tenant, iface)
		return &Lease{gateway: g, tenant: tenant, iface: iface}, nil
	}

	// 第一步：速率桶（平均速率 + 突发）。
	// rateDedup 在整个 Acquire 内保持稳定：即使存储调用超时后由远程层重试，
	// 服务端也只会扣一次令牌；它与并发阶段的 RequestID 相互独立。
	rateDedup := newRequestID()
	rr, err := g.store.TryTakeRate(ctx, RateDeduction{
		Key:      RateKey{Tenant: tenant, Iface: iface},
		Want:     1,
		DedupKey: string(rateDedup),
	})
	if err != nil {
		// 即使失败发生在速率阶段，fail-open 放行也要受同一租户硬上限保护。
		return g.handleDependencyError(ctx, tenant, iface, lim.MaxConcurrent, "rate: "+err.Error())
	}
	if g.metrics != nil {
		g.metrics.RateTaken(tenant, iface, rr.Remaining)
	}
	if !rr.Allowed {
		g.recordRejected(tenant, iface, ReasonRateLimited)
		return nil, newQuotaError(
			ReasonRateLimited,
			"tenant/interface rate or burst exceeded",
			retryDelay(rr.RetryIn))
	}

	// 第二步：并发准入（硬上限 + 组公平）。公平竞争下在 maxWait 内退避重试，
	// 整轮使用同一个 RequestID，存储侧返回首次裁决，绝不重复计数。
	group, hasGroup := cfg.MatchGroup(tenant, iface)
	req := AdmitRequest{
		Tenant:    tenant,
		Iface:     iface,
		RequestID: newRequestID(),
		HardCap:   lim.MaxConcurrent,
		Weight:    lim.Weight,
	}
	if hasGroup {
		req.GroupName = group.Name
		req.GroupCapacity = group.Capacity
		if req.Weight <= 0 {
			req.Weight = group.DefaultWeight
		}
	}
	if req.Weight <= 0 {
		req.Weight = 1
	}

	deadline := g.clock.Now().Add(g.maxWait)
	backoff := defaultBackoff
	var attempt int64
	for {
		req.Attempt = attempt
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ar, err := g.store.Admit(ctx, req)
		if err != nil {
			// 速率令牌已取但并发阶段依赖失败：令牌在窗口内自然回填，
			// 不做跨存储补偿（补偿会引入双写放大）。
			hardCap := lim.MaxConcurrent
			return g.handleDependencyError(ctx, tenant, iface, hardCap, "admit: "+err.Error())
		}
		if g.metrics != nil {
			g.metrics.Concurrency(tenant, iface, ar.Active)
		}
		if ar.Admitted {
			g.recordAllowed(tenant, iface)
			return &Lease{
				gateway: g,
				token:   ar.Token,
				reqID:   req.RequestID,
				tenant:  tenant,
				iface:   iface,
			}, nil
		}
		// 硬上限：立即拒绝，不排队。
		if ar.Reason == ReasonConcurrentLimit {
			g.recordRejected(tenant, iface, ReasonConcurrentLimit)
			return nil, newQuotaError(
				ReasonConcurrentLimit,
				"tenant/interface max concurrent exceeded",
				retryDelay(ar.RetryIn))
		}
		// 幂等键被复用（理论上不会发生，防御性处理）：立即失败。
		if ar.Reason == ReasonRequestReused {
			g.recordRejected(tenant, iface, ReasonRequestReused)
			return nil, newQuotaError(
				ReasonRequestReused,
				"request id already consumed; generate a new id per request",
				retryDelay(ar.RetryIn))
		}
		// 公平排队：带退避的有界重试。
		wait := retryDelay(ar.RetryIn)
		if wait <= 0 {
			wait = backoff
		}
		if g.clock.Now().Add(wait).After(deadline) {
			g.recordRejected(tenant, iface, ReasonFairnessWait)
			return nil, newQuotaError(
				ReasonFairnessWait,
				"capacity contention: fair-scheduling budget exhausted",
				wait)
		}
		if err := g.sleepCtx(ctx, wait); err != nil {
			return nil, err
		}
		if backoff < 250*time.Millisecond {
			backoff *= 2
		}
		attempt++
	}
}

func retryDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d
}

// handleDependencyError 在配额依赖调用失败时执行可解释降级。
func (g *Gateway) handleDependencyError(ctx context.Context, tenant, iface string, hardCap int64, _ string) (*Lease, error) {
	if g.metrics != nil {
		g.metrics.Degraded(tenant, iface, g.degrader.policy)
	}
	if _, err := g.degrader.Enter(ctx, tenant, iface, hardCap); err != nil {
		g.recordRejected(tenant, iface, ReasonDependencyDown)
		return nil, err
	}
	// fail-open：返回本地兜底租约；依赖恢复期间的请求仍受本地硬上限保护。
	return &Lease{gateway: g, tenant: tenant, iface: iface, fallback: true}, nil
}

func (g *Gateway) releaseLease(l *Lease) {
	if g.closed.Load() {
		// 关闭路径：分布式模式服务器会凭会话 TTL 回收；本地模式存储随之销毁。
		return
	}
	if l.fallback {
		g.degrader.Exit(l.tenant, l.iface)
		return
	}
	if l.token == "" {
		return // 未配置限额的空租约
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Release 使用与准入相同的 RequestID：服务端幂等表保证“先过期回收、
	// 后到达 Release”这类乱序/重复路径只执行一次归还。
	if _, err := g.store.Release(ctx, l.token, l.reqID); err != nil {
		// 释放失败不重试不 panic：服务器侧会话/租约 TTL 是最终兜底，
		// 不会泄漏。仅记录可观测事件（不计入“拒绝”指标，避免误导）。
		g.recordReleaseFailure(l.tenant, l.iface)
	}
}

// recordReleaseFailure 记录一次释放失败（协调端将凭 TTL 兜底回收）。
func (g *Gateway) recordReleaseFailure(tenant, iface string) {
	switch m := g.metrics.(type) {
	case *LogMetrics:
		m.releaseFailed(tenant, iface)
	}
}

func (g *Gateway) sleepCtx(ctx context.Context, d time.Duration) error {
	ch, cancel := g.clock.NewTimer(d)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func (g *Gateway) recordAllowed(tenant, iface string) {
	if g.metrics != nil {
		g.metrics.Allowed(tenant, iface)
	}
}

func (g *Gateway) recordRejected(tenant, iface string, r Reason) {
	if g.metrics != nil {
		g.metrics.Rejected(tenant, iface, r)
	}
}

// ReplaceConfig 热更新配置：
//  1. 先做本地静态校验（非法/不兼容立即拒绝，当前配置不受影响）；
//  2. 再同步到存储侧（分布式下经协调服务再次校验，保证多实例一致）；
//  3. 存储侧安装成功后，才原子提交本地视图。
//
// 因此存储不可用或拒绝配置时，正在使用的配置完全不变，不会出现语义分裂。
func (g *Gateway) ReplaceConfig(ctx context.Context, cfg *Config) error {
	if err := ValidateConfig(cfg); err != nil {
		if g.metrics != nil {
			g.metrics.ConfigReloaded(cfgVersion(cfg), false)
		}
		return err
	}
	syncCtx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	if err := g.store.SyncConfig(syncCtx, cfg); err != nil {
		if g.metrics != nil {
			g.metrics.ConfigReloaded(cfg.Version, false)
		}
		return err
	}
	return g.mgr.Replace(ctx, cfg)
}

// CurrentConfig 暴露当前生效配置（观测/测试用）。
func (g *Gateway) CurrentConfig() *Config { return g.mgr.Current() }

// Close 关闭网关及后台资源。
func (g *Gateway) Close() error {
	var err error
	g.closeOnce.Do(func() {
		g.closed.Store(true)
		err = g.store.Close()
	})
	return err
}

// IsDegraded 返回配额依赖当前是否处于降级状态。
func (g *Gateway) IsDegraded() bool { return g.degrader.IsDegraded() }
