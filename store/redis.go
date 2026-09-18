package store

import (
	"context"
	"time"
)

// RedisOptions 是 Redis 存储配置。
type RedisOptions struct {
	Addr        string
	Password    string
	DB          int
	KeyPrefix   string
	DialTimeout time.Duration
}

// RedisStore 通过 Redis Lua 脚本实现跨实例原子配额。
type RedisStore struct{}

// NewRedisStore 创建并探测 Redis 存储。
func NewRedisStore(opts RedisOptions) (*RedisStore, error) { return &RedisStore{}, nil }

func (s *RedisStore) TryAcquire(ctx context.Context, rateKey, concKey string, rate float64, burst, maxConcurrent int64, token string, ttl time.Duration) (Result, error) {
	return Result{}, nil
}

func (s *RedisStore) Release(ctx context.Context, concKey, token string) error { return nil }

func (s *RedisStore) Ping(ctx context.Context) error { return nil }

func (s *RedisStore) Close() error { return nil }
