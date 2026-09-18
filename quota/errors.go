// Package quota 提供多租户配额与公平调度能力。
package quota

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
//
// 该类型实现 net/error 与 errors.As 可识别的错误，调用方可据 Code
// 区分限流、并发超限、后端不可用降级拒绝等场景，并据 RetryAfter 生成
// Retry-After 提示。
type Error struct {
	Code       ErrorCode
	Tenant     string
	API        string
	Message    string
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("quota: ")
	b.WriteString(string(e.Code))
	if e.Tenant != "" {
		b.WriteString(" tenant=" + e.Tenant)
	}
	if e.API != "" {
		b.WriteString(" api=" + e.API)
	}
	if e.Message != "" {
		b.WriteString(": " + e.Message)
	}
	if e.RetryAfter > 0 {
		b.WriteString(" retry_after=" + e.RetryAfter.String())
	}
	return b.String()
}

// RetryAfterSeconds 返回向上取整的 Retry-After 秒数；0 表示暂无建议。
func (e *Error) RetryAfterSeconds() int {
	if e.RetryAfter <= 0 {
		return 0
	}
	return int((e.RetryAfter + time.Second - 1) / time.Second)
}

// AsError 从任意 error 中提取 *Error。
func AsError(err error) (*Error, bool) {
	var qe *Error
	if errors.As(err, &qe) {
		return qe, true
	}
	return nil, false
}

func newRateLimitedError(tenant, api string, retryAfter time.Duration) *Error {
	return &Error{
		Code:       ErrCodeRateLimited,
		Tenant:     tenant,
		API:        api,
		Message:    "tenant rate or burst limit exceeded",
		RetryAfter: retryAfter,
	}
}

func newConcurrencyLimitedError(tenant, api string, retryAfter time.Duration) *Error {
	return &Error{
		Code:       ErrCodeConcurrencyLimited,
		Tenant:     tenant,
		API:        api,
		Message:    "tenant concurrent in-flight limit exceeded",
		RetryAfter: retryAfter,
	}
}

func newUnavailableError(tenant, api string, cause error) *Error {
	msg := "quota backend unavailable"
	if cause != nil {
		msg = fmt.Sprintf("%s: %v", msg, cause)
	}
	return &Error{
		Code:    ErrCodeUnavailable,
		Tenant:  tenant,
		API:     api,
		Message: msg,
	}
}

func newDegradedDenyError(tenant, api string) *Error {
	return &Error{
		Code:    ErrCodeDegradedDeny,
		Tenant:  tenant,
		API:     api,
		Message: "quota backend unavailable and degrade policy is deny",
	}
}

// configError 表示配置校验失败。
type configError struct {
	reason string
	field  string
}

func (e *configError) Error() string {
	if e.field == "" {
		return "quota: invalid config: " + e.reason
	}
	return "quota: invalid config: " + e.field + ": " + e.reason
}

func newConfigError(field, msg string) error {
	return &configError{field: field, reason: msg}
}

func newConfigErrorf(field, format string, args ...any) error {
	return &configError{field: field, reason: fmt.Sprintf(format, args...)}
}

// isConfigError 报告 err 是否为配置校验错误（便于测试断言）。
func isConfigError(err error) bool {
	var ce *configError
	return errors.As(err, &ce)
}

// itoa 是 strconv.Itoa 的简短封装，供本包使用。
func itoa(n int) string { return strconv.Itoa(n) }
