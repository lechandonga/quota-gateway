package store

import (
	"context"
	"time"
)

// MemoryStore 是单实例内存实现（也用于测试）。
type MemoryStore struct{}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) TryAcquire(ctx context.Context, rateKey, concKey string, rate float64, burst, maxConcurrent int64, token string, ttl time.Duration) (Result, error) {
	return Result{}, nil
}

func (s *MemoryStore) Release(ctx context.Context, concKey, token string) error { return nil }

func (s *MemoryStore) Ping(ctx context.Context) error { return nil }

func (s *MemoryStore) Close() error { return nil }
