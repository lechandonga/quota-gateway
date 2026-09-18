package quota

import "time"

// tokenBucket 是惰性补充的令牌桶（仅用于本地预估/单实例场景）。
type tokenBucket struct {
	rate     float64
	capacity float64
	tokens   float64
	last     time.Time
}

func newTokenBucket(rate float64, burst int64, now time.Time) *tokenBucket { return nil }

// take 尝试取走 1 个令牌。
func (b *tokenBucket) take(now time.Time) bool { return false }

// retryAfter 返回等待 1 个令牌所需时间。
func (b *tokenBucket) retryAfter(now time.Time) time.Duration { return 0 }
