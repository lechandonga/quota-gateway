// Package quota 提供多租户配额与公平调度能力。
package quota

import (
	"time"
)

// ErrorCode 是可区分的配额错误码。
type ErrorCode string

// 预定义错误码。
const (
	ErrCodeRateLimited        ErrorCode = "rate_limited"
	ErrCodeConcurrencyLimited ErrorCode = "concurrency_limited"
	ErrCodeUnavailable        ErrorCode = "quota_backend_unavailable"
	ErrCodeDegradedDeny       ErrorCode = "degraded_deny"
	ErrCodeInvalidConfig      ErrorCode = "invalid_config"
	ErrCodeInternal           ErrorCode = "internal"
)

// Error 描述一次配额拒绝或降级事件。
type Error struct {
	Code       ErrorCode
	Tenant     string
	API        string
	Message    string
	RetryAfter time.Duration
}

func (e *Error) Error() string { return "" }

// AsError 从 error 中提取 *Error。
func AsError(err error) (*Error, bool) { return nil, false }

func newRateLimitedError(tenant, api string, retryAfter time.Duration) *Error { return nil }

func newConcurrencyLimitedError(tenant, api string, retryAfter time.Duration) *Error { return nil }

func newUnavailableError(tenant, api string, cause error) *Error { return nil }

func newDegradedDenyError(tenant, api string) *Error { return nil }
