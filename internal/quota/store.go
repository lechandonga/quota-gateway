package quota

import (
	"context"
	"time"
)

// SessionID 标识一个网关实例与协调存储之间的会话。
type SessionID string

// RequestID 是调用方生成的幂等键，保证网络重试不重复计数。
type RequestID string

// LeaseToken 是一次并发占用的不透明凭证，释放凭此完成。
type LeaseToken string

// RateKey 定位一个租户/接口维度的速率桶。
type RateKey struct {
	Tenant string
	Iface  string
}

// RateResult 是速率扣减结果。
type RateResult struct {
	Allowed       bool
	Remaining     int64
	RetryIn       time.Duration
	ConfigVersion int64
}

// RateDeduction 是一次速率扣减申请。DedupKey 由调用方为每个逻辑请求生成，
// 使“超时后重发”在服务端只扣一次令牌，避免一次请求被重复计数。
type RateDeduction struct {
	Key      RateKey
	Want     int64
	DedupKey string
}

// AdmitRequest 描述一次并发准入申请（参数由网关侧当前配置计算）。
type AdmitRequest struct {
	Tenant        string
	Iface         string
	RequestID     RequestID // 整个逻辑申请的稳定幂等键（含全部公平重试）
	Attempt       int64     // 公平排队的第几轮尝试；网络重试保持不变，排队重试图新值
	HardCap       int64     // 0 表示不限制
	GroupName     string
	GroupCapacity int64 // 仅当 GroupName != "" 时有效
	Weight        float64
}

// AdmitResult 是准入裁决结果。
type AdmitResult struct {
	Admitted      bool
	Token         LeaseToken
	Reason        Reason
	RetryIn       time.Duration
	Active        int64 // 该租户/接口当前被占用的并发数
	ConfigVersion int64
}

// ReleaseResult 是释放结果。
type ReleaseResult struct {
	Released      bool // false 表示凭证未知或已释放（幂等）
	ConfigVersion int64
}

// Store 是配额状态存储抽象：本地内存与分布式共享存储统一接口。
// 所有方法都必须对网络重试安全（基于 RequestID/LeaseToken 幂等）。
type Store interface {
	// SyncConfig 在存储侧校验并安装新配置；非法配置必须返回错误且保持旧配置不变。
	SyncConfig(ctx context.Context, cfg *Config) error
	// TryTakeRate 从速率桶尝试取走 want 个令牌。
	// DedupKey 保证同一逻辑请求的传输重试只扣一次。
	TryTakeRate(ctx context.Context, ded RateDeduction) (RateResult, error)
	// Admit 申请一个并发名额。
	Admit(ctx context.Context, req AdmitRequest) (AdmitResult, error)
	// Release 归还并发名额，重复/乱序/未知凭证均安全。
	Release(ctx context.Context, token LeaseToken, reqID RequestID) (ReleaseResult, error)
	// Close 释放存储资源（心跳协程等）。
	Close() error
}
