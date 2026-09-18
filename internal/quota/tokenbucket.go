package quota

import (
	"math"
	"sync"
	"time"
)

// TokenBucket 是惰性填充的令牌桶（线程安全）。
// 不做后台协程：仅在 Take/Update 时按经过时间补充令牌。
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens / second
	burst  float64
	tokens float64 // 当前可用令牌（可为浮点）
	last   time.Time
}

// NewTokenBucket 以给定速率与突发容量构造令牌桶，初始桶满。
func NewTokenBucket(ratePerSec float64, burst int64, now time.Time) *TokenBucket {
	return &TokenBucket{
		rate:   ratePerSec,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   now,
	}
}

// Update 热更新速率与突发容量。
// 新速率立即用于填充；突发缩小时按比例收敛（不超过新上限），
// 突发放大不补发历史令牌（避免更新瞬间凭空产生额度）。
func (b *TokenBucket) Update(ratePerSec float64, burst int64, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(now)
	b.rate = ratePerSec
	nb := float64(burst)
	b.burst = nb
	if b.tokens > nb {
		b.tokens = nb
	}
}

func (b *TokenBucket) refill(now time.Time) {
	if now.After(b.last) {
		b.tokens = math.Min(b.burst, b.tokens+b.rate*now.Sub(b.last).Seconds())
		b.last = now
	}
}

// Take 尝试取走 n 个令牌；不允许时返回需要等待的时间。
// remaining 为取后剩余令牌（向下取整，供指标使用）。
func (b *TokenBucket) Take(n int64, now time.Time) (allowed bool, retryAfter time.Duration, remaining int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(now)
	need := float64(n)
	if b.tokens+1e-9 >= need {
		b.tokens -= need
		return true, 0, int64(math.Floor(b.tokens))
	}
	missing := need - b.tokens
	if b.rate <= 0 {
		// 理论上不会发生（校验保证 rate>0），防御性处理
		return false, time.Hour, int64(math.Floor(b.tokens))
	}
	secs := missing / b.rate
	return false, time.Duration(secs * float64(time.Second)), int64(math.Floor(b.tokens))
}
