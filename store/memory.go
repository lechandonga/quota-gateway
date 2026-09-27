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

// Renew 批量延长占位过期时间；仅刷新仍存活（未过期、未释放）的占位，
// 已过期/不存在的 token 不复活，避免名额被重复计入。
//
// 返回在 keys 中“并非每个键都仍持有”的 token：即任一占位键上已经
// 丢失（过期/被删/从未存在）的 token，供调用方做丢失感知与补占。
func (s *MemoryStore) Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ttl <= 0 {
		return nil, nil // 无 TTL 的占位不过期，无需续租
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var missing []string
	for _, tok := range tokens {
		lost := false
		for _, k := range keys {
			if k == "" {
				continue
			}
			holders := s.conc[k]
			if holders == nil {
				lost = true
				continue
			}
			s.expireLocked(holders, now)
			if _, ok := holders[tok]; ok {
				holders[tok] = now.Add(ttl)
			} else {
				lost = true
			}
		}
		// 只要在某个应持有的键上不存在即上报（含全部键均已丢失）；
		// 已正常 Release 的 token 也可能出现在这里，由调用方按本地
		// 租约状态自行过滤。
		if lost {
			missing = append(missing, tok)
		}
	}
	return missing, nil
}

// Reclaim 在不触碰速率令牌的前提下，按当前上限原子补占丢失的 token。
// 租户键为空（MaxConcurrent<=0 的规则、仅受共享池约束）时只补占共享键；
// 任一启用的键达到对应上限则整体不写入，返回受限状态供调用方稍后重试。
func (s *MemoryStore) Reclaim(ctx context.Context, req ReclaimRequest) (Status, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	var tenantHolders map[string]time.Time
	if req.ConcKey != "" && req.MaxConcurrent > 0 {
		tenantHolders = s.holdersLocked(req.ConcKey)
		s.expireLocked(tenantHolders, now)
		if int64(len(tenantHolders)) >= req.MaxConcurrent {
			return StatusConcurrencyLimited, nil
		}
	}
	var sharedHolders map[string]time.Time
	if req.SharedKey != "" && req.MaxShared > 0 {
		sharedHolders = s.holdersLocked(req.SharedKey)
		s.expireLocked(sharedHolders, now)
		if int64(len(sharedHolders)) >= req.MaxShared {
			return StatusSharedLimited, nil
		}
	}
	if tenantHolders != nil {
		s.putLocked(tenantHolders, req.Token, now, req.TTL)
	}
	if sharedHolders != nil {
		s.putLocked(sharedHolders, req.Token, now, req.TTL)
	}
	return StatusAcquired, nil
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

// LoseHolders 删除 token 在所有占位键上的记录：测试用，模拟后端抖动/
// 重启导致“在途请求仍在处理、后端名额记录中途丢失”。
func (s *MemoryStore) LoseHolders(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, holders := range s.conc {
		if _, ok := holders[token]; ok {
			delete(holders, token)
			if len(holders) == 0 {
				delete(s.conc, k)
			}
		}
	}
}
