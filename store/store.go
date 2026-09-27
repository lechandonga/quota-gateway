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

// ReclaimRequest 是一次“丢失名额补占”的参数（仅并发键，不涉及速率）。
type ReclaimRequest struct {
	ConcKey       string // 租户维度并发键（必填）
	SharedKey     string // 共享容量池键；空串表示不参与共享池
	MaxConcurrent int64  // 租户并发硬上限，<=0 不限
	MaxShared     int64  // 共享容量池大小，<=0 或 SharedKey 为空时不限
	Token         string
	TTL           time.Duration
}

// Store 是配额状态存储。所有实现必须保证 TryAcquire 的原子性。
type Store interface {
	// TryAcquire 原子地完成：速率令牌扣减 + 租户并发占位 + 共享容量占位。
	TryAcquire(ctx context.Context, req AcquireRequest) (Result, error)

	// Release 幂等释放凭证在给定键上的并发占位（同时释放租户键与共享键）。
	// 重复释放与乱序释放必须无害；速率令牌不归还。
	Release(ctx context.Context, keys []string, token string) error

	// Renew 批量续租：把 keys 中仍然存在的 tokens 占位的过期时间延长到
	// 现在+ttl。已过期或已释放的占位不得复活（防止名额被重复计入）。
	// 用于长连接场景：只要持有方存活就持续续租，崩溃后占位在 TTL 内
	// 有界回收。
	//
	// 返回值 missing 是在 keys 中任一占位键上已经不存在的 token 子集
	// （如后端抖动/重启导致记录丢失、或记录过期被惰性回收）：调用方据此
	// 感知“在途请求仍在处理、名额却已不在后端”的事件，并用 Reclaim
	// 在不超上限的前提下把名额占回来。missing 可能包含本实例已正常
	// Release 的 token（续租批次与释放并发交错），调用方必须自行按本地
	// 租约状态过滤。
	Renew(ctx context.Context, keys []string, tokens []string, ttl time.Duration) (missing []string, err error)

	// Reclaim 在“持有方仍在处理、但后端占位记录已丢失”时按当前上限
	// 原子地补占同一个 token：
	//   - 不触碰速率令牌（请求早已通过速率判定并在服务中）；
	//   - 仅当 token 仍缺失的每个键当前有效占位数低于对应上限时才占位，
	//     成功后这些键上的总数严格不超过 maxConc/maxShared（不放大额度）；
	//   - 任一键已满则整体不写入任何键（StatusConcurrencyLimited 或
	//     StatusSharedLimited），调用方应在下个续租周期重试；
	//   - token 若已被其他路径重新占回，则视为成功（StatusAcquired），
	//     占位 TTL 统一刷新到现在+ttl。
	Reclaim(ctx context.Context, req ReclaimRequest) (Status, error)

	// Ping 探测后端可用性。
	Ping(ctx context.Context) error

	Close() error
}
