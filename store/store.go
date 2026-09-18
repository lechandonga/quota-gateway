// Package store 定义配额状态的原子存储抽象。
//
// 该抽象保证“速率令牌扣减 + 并发占位”在多实例并发下以单条原子操作完成，
// 从而避免额度放大或重复计数。
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
	StatusConcurrencyLimited
)

// Result 是 TryAcquire 的返回值。
type Result struct {
	Status     Status
	RetryAfter time.Duration
	// Token 是本次并发占位的唯一凭证，释放时必须原样带回。
	Token string
}

// Store 是配额状态存储。所有实现必须保证 TryAcquire 的原子性。
type Store interface {
	// TryAcquire 原子地完成：速率令牌扣减 + 并发占位。
	// rate<=0 表示不限速；maxConcurrent<=0 表示不限并发。
	// ttl 为并发占位的过期时间，用于实例崩溃后的占位回收。
	TryAcquire(ctx context.Context, rateKey, concKey string, rate float64, burst, maxConcurrent int64, token string, ttl time.Duration) (Result, error)

	// Release 释放指定凭证的并发占位；重复释放与乱序释放必须无害。
	// 速率令牌不归还（请求未被服务，不应回补）。
	Release(ctx context.Context, concKey, token string) error

	// Ping 探测后端可用性。
	Ping(ctx context.Context) error

	Close() error
}
