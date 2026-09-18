package quota

import (
	"math"
	"sync"
	"time"
)

// tokenBucket 是惰性补充的令牌桶。
//
// 它只在每次 take 时按经过时间补充令牌，不需要后台 goroutine，
// 因此不会产生泄漏；rate<=0 表示“不限速”（恒可取用）。
type tokenBucket struct {
	mu       sync.Mutex
	rate     float64 // 令牌/秒
	capacity float64
	tokens   float64
	last     time.Time
}

func newTokenBucket(rate float64, burst int64, now time.Time) *tokenBucket {
	cap := float64(burst)
	return &tokenBucket{
		rate:     rate,
		capacity: cap,
		tokens:   cap, // 新桶初始为满，允许冷启动突发
		last:     now,
	}
}

// refill 按经过时间补充令牌，调用方需持有 mu。
func (b *tokenBucket) refill(now time.Time) {
	if b.rate <= 0 {
		// 不限速：始终保持“满”。
		b.tokens = b.capacity
		b.last = now
		return
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens = math.Min(b.capacity, b.tokens+elapsed*b.rate)
	b.last = now
}

// take 尝试取走 1 个令牌；成功返回 true。
func (b *tokenBucket) take(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate <= 0 {
		return true
	}
	b.refill(now)
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// retryAfter 返回距离下一个令牌可用的等待时间（已取锁）。
func (b *tokenBucket) retryAfter(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate <= 0 {
		return 0
	}
	b.refill(now)
	if b.tokens >= 1 {
		return 0
	}
	need := (1 - b.tokens) / b.rate
	d := time.Duration(need * float64(time.Second))
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// state 返回当前令牌数与补充到给定数量所需时间，供原子判定一次性使用。
// needed 为尚缺令牌数（>=0）。
func (b *tokenBucket) state(now time.Time) (tokens float64, needed int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate <= 0 {
		return b.capacity, 0
	}
	b.refill(now)
	if b.tokens >= 1 {
		return b.tokens, 0
	}
	return b.tokens, 1
}
