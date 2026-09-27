// Package store 定义配额状态的原子存储抽象。
//
// TryAcquire 以单条原子操作完成“速率令牌扣减 + 租户并发占位 + 共享容量占位”，
// 多实例并发下既不会额度放大，也不会重复计数。
package store

import (
	"context"
	"time"
)

// Status 是 TryAcquire 的原子判定结果。
type Status int

const (
	StatusAcquired Status = iota
	StatusRateLimited
	StatusConcurrencyLimited // 命中租户自身并发硬上限：调用方应立即拒绝
	StatusSharedLimited      // 共享容量池暂时占满：调用方可公平排队后重试
)

// AcquireRequest 是一次原子获取的参数。
type AcquireRequest struct {
	RateKey       string
	ConcKey       string // 租户维度并发键
	SharedKey     string // 共享容量池键；空串表示不参与共享池
	Rate          float64
	Burst         int64
	MaxConcurrent int64 // 租户并发硬上限，<=0 不限
	MaxShared     int64 // 共享容量池大小，<=0 或 SharedKey 为空时不限
	Token         string
	TTL           time.Duration
}

// Result 是 TryAcquire 的返回值。
type Result struct {
	Status     Status
	RetryAfter time.Duration
	// Token 是本次并发占位的唯一凭证，释放时必须原样带回。
	Token string
}

// Store 是配额状态存储。所有实现必须保证 TryAcquire 的原子性。
type Store interface {
	// TryAcquire 原子地完成：速率令牌扣减 + 租户并发占位 + 共享容量占位。
	TryAcquire(ctx context.Context, req AcquireRequest) (Result, error)

	// Release 幂等释放凭证在给定键上的并发占位（同时释放租户键与共享键）。
	// 重复释放与乱序释放必须无害；速率令牌不归还。
	Release(ctx context.Context, keys []string, token string) error

	// Renew 批量续租（确保占位存在）：把 keys 中 tokens 占位的过期时间
	// 延长到 现在+ttl；后端记录已丢失（重启/抖动/过期）的占位按原样
	// 重建，并以 lost 返回这些 token（用于观测“名额中途丢失”事件）。
	//
	// 调用方必须只对“本地仍登记为在途”的 token 续租：已释放的 token
	// 不得再传入。管理器在释放前先摘除本地租约，释放与续租的并发交错
	// 最多造成一个 TTL 周期内的占位残留（有界，不会泄漏）。
	//
	// 用于长连接场景：只要持有方存活就持续续租，请求在途多久名额都不
	// 会被提前归还；持有方崩溃后占位在 TTL 内有界回收。
	Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) (lost []string, err error)

	// Ping 探测后端可用性。
	Ping(ctx context.Context) error

	Close() error
}
