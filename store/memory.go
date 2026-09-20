package store

import (
	"context"
	"math"
	"sync"
	"time"
)

// minConcurrencyRetry 是并发超限时的最小重试提示。
const minConcurrencyRetry = 10 * time.Millisecond

type rateState struct {
	rate     float64
	capacity float64
	tokens   float64
	last     time.Time
}

// MemoryStore 是单实例内存实现，TryAcquire 在单把互斥锁下原子完成
// “速率令牌扣减 + 租户并发占位 + 共享容量占位”，语义与 Redis Lua 一致。
//
// 并发占位带 TTL：即使持有者所在实例崩溃、未调用 Release，过期占位也会
// 在后续判定时惰性回收，不会造成额度泄漏。
type MemoryStore struct {
	mu   sync.Mutex
	now  func() time.Time
	rate map[string]*rateState
	conc map[string]map[string]time.Time // 键 -> token -> 过期时刻
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		now:  time.Now,
		rate: make(map[string]*rateState),
		conc: make(map[string]map[string]time.Time),
	}
}

// SetNow 注入时钟（仅用于测试）。
func (s *MemoryStore) SetNow(f func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = f
}

func (s *MemoryStore) TryAcquire(ctx context.Context, req AcquireRequest) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// ---- 1) 速率判定（不通过则立即拒绝，不产生任何占位） ----
	if req.Rate > 0 {
		b, ok := s.rate[req.RateKey]
		if !ok {
			b = &rateState{rate: req.Rate, capacity: float64(req.Burst), tokens: float64(req.Burst), last: now}
			s.rate[req.RateKey] = b
		}
		// 配置热更新：参数即时生效；容量缩小时令牌封顶，放大时不额外补发，
		// 避免“改配置放大额度”。
		b.rate = req.Rate
		b.capacity = float64(req.Burst)
		s.refillLocked(b, now)
		if b.tokens < 1 {
			retry := time.Duration((1-b.tokens)/req.Rate*float64(time.Second)) + time.Millisecond
			return Result{Status: StatusRateLimited, RetryAfter: retry}, nil
		}
	}

	// ---- 2) 租户并发硬上限（超限立即拒绝，不应排队） ----
	tenantHolders := s.holdersLocked(req.ConcKey)
	s.expireLocked(tenantHolders, now)
	if req.MaxConcurrent > 0 && int64(len(tenantHolders)) >= req.MaxConcurrent {
		return Result{Status: StatusConcurrencyLimited, RetryAfter: s.earliestFreeLocked(tenantHolders, now)}, nil
	}

	// ---- 3) 共享容量池（占满时返回可排队状态，供公平调度） ----
	var sharedHolders map[string]time.Time
	if req.SharedKey != "" && req.MaxShared > 0 {
		sharedHolders = s.holdersLocked(req.SharedKey)
		s.expireLocked(sharedHolders, now)
		if int64(len(sharedHolders)) >= req.MaxShared {
			return Result{Status: StatusSharedLimited, RetryAfter: s.earliestFreeLocked(sharedHolders, now)}, nil
		}
	}

	// ---- 4) 提交：扣令牌 + 两个键同时占位（同一把锁，原子可见） ----
	if req.Rate > 0 {
		s.rate[req.RateKey].tokens--
	}
	if req.MaxConcurrent > 0 {
		s.putLocked(tenantHolders, req.Token, now, req.TTL)
	}
	if sharedHolders != nil {
		s.putLocked(sharedHolders, req.Token, now, req.TTL)
	}
	return Result{Status: StatusAcquired, Token: req.Token}, nil
}

func (s *MemoryStore) Release(ctx context.Context, keys []string, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		if k == "" {
			continue
		}
		holders := s.conc[k]
		if holders == nil {
			continue // 重复/乱序释放：无副作用
		}
		delete(holders, token) // 幂等：仅当 token 仍存在时生效
		if len(holders) == 0 {
			delete(s.conc, k)
		}
	}
	return nil
}

// Renew 续期凭证在所有给定键上的占位；任一键上凭证已丢失（被释放或已
// 过期惰性回收）则整体不修改并返回 false，语义与 Redis Lua 一致。
func (s *MemoryStore) Renew(ctx context.Context, keys []string, token string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.renewLocked(keys, token, ttl, now) {
		return false, nil
	}
	return true, nil
}

// RenewAll 在同一把锁内原子批量续租多个凭证；返回未能续期（缺失/过期）
// 的凭证 token 列表。适用于数千长连接由单个续租器统一续期的场景，避免
// 逐个加锁造成单轮扫描在短 TTL 下超时。
func (s *MemoryStore) RenewAll(ctx context.Context, items []RenewItem) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var missing []string
	for _, it := range items {
		if !s.renewLocked(it.Keys, it.Token, it.TTL, now) {
			missing = append(missing, it.Token)
		}
	}
	return missing, nil
}

// renewLocked 要求持有 s.mu：校验凭证在所有键上存在且未过期，然后整体延长。
func (s *MemoryStore) renewLocked(keys []string, token string, ttl time.Duration, now time.Time) bool {
	validKeys := 0
	for _, k := range keys {
		if k != "" {
			validKeys++
		}
	}
	if validKeys == 0 {
		return false
	}
	// 1) 校验：每个键上凭证都必须存在且未过期。
	for _, k := range keys {
		if k == "" {
			continue
		}
		holders := s.conc[k]
		exp, ok := holders[token]
		if !ok {
			return false
		}
		if !exp.IsZero() && !now.Before(exp) {
			delete(holders, token) // 已过期：顺带惰性清理
			if len(holders) == 0 {
				delete(s.conc, k)
			}
			return false
		}
	}
	// 2) 提交：同一把锁内整体延长。
	newExp := time.Time{}
	if ttl > 0 {
		newExp = now.Add(ttl)
	}
	for _, k := range keys {
		if k == "" {
			continue
		}
		s.conc[k][token] = newExp
	}
	return true
}

func (s *MemoryStore) Ping(ctx context.Context) error { return ctx.Err() }

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rate = make(map[string]*rateState)
	s.conc = make(map[string]map[string]time.Time)
	return nil
}

// holdersLocked 返回键对应的占位表（惰性创建）。
func (s *MemoryStore) holdersLocked(key string) map[string]time.Time {
	if key == "" {
		return nil
	}
	h := s.conc[key]
	if h == nil {
		h = make(map[string]time.Time)
		s.conc[key] = h
	}
	return h
}

func (s *MemoryStore) putLocked(holders map[string]time.Time, token string, now time.Time, ttl time.Duration) {
	if ttl > 0 {
		holders[token] = now.Add(ttl)
	} else {
		holders[token] = time.Time{} // 无 TTL：仅靠 Release 释放
	}
}

func (s *MemoryStore) refillLocked(b *rateState, now time.Time) {
	if b.rate <= 0 {
		return
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(b.capacity, b.tokens+elapsed*b.rate)
		b.last = now
	}
}

func (s *MemoryStore) expireLocked(holders map[string]time.Time, now time.Time) {
	for tok, exp := range holders {
		if !exp.IsZero() && !now.Before(exp) {
			delete(holders, tok)
		}
	}
}

// earliestFreeLocked 返回最近一个占位到期的剩余时间（并发重试提示）。
func (s *MemoryStore) earliestFreeLocked(holders map[string]time.Time, now time.Time) time.Duration {
	d := minConcurrencyRetry
	for _, exp := range holders {
		if exp.IsZero() {
			continue
		}
		rem := exp.Sub(now)
		if rem < d {
			d = rem
		}
	}
	if d <= 0 {
		d = minConcurrencyRetry
	}
	return d
}

// inflight 返回某键的有效占位数（仅用于测试/观测）。
func (s *MemoryStore) inflight(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(s.conc[key], s.now())
	return len(s.conc[key])
}

// Inflight 返回某键当前有效占位数，供测试与运维观测使用。
func (s *MemoryStore) Inflight(key string) int { return s.inflight(key) }
