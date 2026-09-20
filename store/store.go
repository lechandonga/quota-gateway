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

// RenewItem 是一个待续租凭证及其占位键，供 RenewAll 批量续租使用。
type RenewItem struct {
	Token string
	Keys  []string
	TTL   time.Duration
}

// Store 是配额状态存储。所有实现必须保证 TryAcquire 的原子性。
type Store interface {
	// TryAcquire 原子地完成：速率令牌扣减 + 租户并发占位 + 共享容量占位。
	TryAcquire(ctx context.Context, req AcquireRequest) (Result, error)

	// Release 幂等释放凭证在给定键上的并发占位（同时释放租户键与共享键）。
	// 重复释放与乱序释放必须无害；速率令牌不归还。
	Release(ctx context.Context, keys []string, token string) error

	// Renew 为仍在处理中的长请求续期凭证占位，把凭证在所有给定键上的
	// 过期时刻延长到 now+ttl（ttl<=0 表示不过期）。
	//
	// 仅当凭证在*每个*键上都仍存在且未过期时才整体续期并返回 renewed=true；
	// 任一键上凭证缺失（已被 Release 删除或已被 TTL 惰性回收）时不做任何
	// 修改并返回 renewed=false——调用方据此知道该凭证已丢失，不得再用它占位。
	Renew(ctx context.Context, keys []string, token string, ttl time.Duration) (renewed bool, err error)

	// RenewAll 批量续租多个凭证（单实例续租器统一调用）：原子地为每个
	// 凭证执行与 Renew 相同的判定+续期，返回未能续期（缺失/过期）的 token
	// 列表；网络/后端错误时返回 error，由调用方下周期重试。
	RenewAll(ctx context.Context, items []RenewItem) (missing []string, err error)

	// Ping 探测后端可用性。
	Ping(ctx context.Context) error

	Close() error
}
