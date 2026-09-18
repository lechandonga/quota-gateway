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
// “速率令牌扣减 + 并发占位”，语义与 Redis Lua 脚本保持一致。
//
// 并发占位带 TTL：即使持有者所在实例崩溃、未调用 Release，
// 过期占位也会在后续判定时被惰性回收，不会造成额度泄漏。
type MemoryStore struct {
	mu   sync.Mutex
	now  func() time.Time
	rate map[string]*rateState
	conc map[string]map[string]time.Time // concKey -> token -> 过期时刻
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

func (s *MemoryStore) TryAcquire(ctx context.Context, rateKey, concKey string, rate float64, burst, maxConcurrent int64, token string, ttl time.Duration) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// ---- 速率判定 ----
	if rate > 0 {
		b, ok := s.rate[rateKey]
		if !ok {
			b = &rateState{rate: rate, capacity: float64(burst), tokens: float64(burst), last: now}
			s.rate[rateKey] = b
		}
		// 配置热更新：新参数即时生效；容量缩小时令牌随之封顶，
		// 容量放大时不额外补发（仅由时间累积），避免“改配置放大额度”。
		b.rate = rate
		b.capacity = float64(burst)
		s.refillLocked(b, now)
		if b.tokens < 1 {
			retry := time.Duration((1-b.tokens)/rate*float64(time.Second)) + time.Millisecond
			return Result{Status: StatusRateLimited, RetryAfter: retry}, nil
		}
	}

	// ---- 并发判定 ----
	holders := s.conc[concKey]
	if holders == nil {
		holders = make(map[string]time.Time)
		s.conc[concKey] = holders
	}
	s.expireLocked(holders, now)
	if maxConcurrent > 0 && int64(len(holders)) >= maxConcurrent {
		return Result{Status: StatusConcurrencyLimited, RetryAfter: s.earliestFreeLocked(holders, now)}, nil
	}

	// ---- 提交：扣令牌 + 占位（同一把锁，原子可见） ----
	if rate > 0 {
		b := s.rate[rateKey]
		b.tokens--
	}
	if maxConcurrent > 0 {
		if ttl > 0 {
			holders[token] = now.Add(ttl)
		} else {
			holders[token] = time.Time{} // 无 TTL：仅靠 Release 释放
		}
	}
	return Result{Status: StatusAcquired, Token: token}, nil
}

func (s *MemoryStore) Release(ctx context.Context, concKey, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	holders := s.conc[concKey]
	if holders == nil {
		return nil // 重复/乱序释放：无副作用
	}
	delete(holders, token) // 幂等：仅当 token 仍存在时生效
	if len(holders) == 0 {
		delete(s.conc, concKey)
	}
	return nil
}

func (s *MemoryStore) Ping(ctx context.Context) error { return ctx.Err() }

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rate = make(map[string]*rateState)
	s.conc = make(map[string]map[string]time.Time)
	return nil
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

// inflight 返回某 concKey 的有效占位数（仅用于测试/观测）。
func (s *MemoryStore) inflight(concKey string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(s.conc[concKey], s.now())
	return len(s.conc[concKey])
}
