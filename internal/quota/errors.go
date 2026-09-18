package quota

import (
	"errors"
	"time"
)

// Reason 是可区分的拒绝原因。
type Reason string

const (
	ReasonRateLimited     Reason = "rate_limited"     // 超过平均速率/突发
	ReasonConcurrentLimit Reason = "concurrent_limit" // 超过租户并发硬上限
	ReasonFairnessWait    Reason = "fairness_wait"    // 容量竞争，按公平调度排队后仍未获取
	ReasonDependencyDown  Reason = "dependency_down"  // 配额依赖不可用且 fail-closed
	ReasonBadConfig       Reason = "bad_config"       // 配置非法或不兼容
	ReasonRequestReused   Reason = "request_reused"   // 幂等键对应的名额已释放/消费
)

// QuotaError 携带稳定的错误码、可机读原因与建议重试等待。
type QuotaError struct {
	Reason  Reason
	Message string
	RetryIn time.Duration // >0 时表示建议客户端等待后重试
}

func (e *QuotaError) Error() string {
	s := "quota: " + string(e.Reason)
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.RetryIn > 0 {
		s += " (retry after " + e.RetryIn.String() + ")"
	}
	return s
}

func newQuotaError(reason Reason, msg string, retryIn time.Duration) *QuotaError {
	return &QuotaError{Reason: reason, Message: msg, RetryIn: retryIn}
}

// AsQuotaError 从错误链中提取 *QuotaError。
func AsQuotaError(err error) (*QuotaError, bool) {
	var qe *QuotaError
	if errors.As(err, &qe) {
		return qe, true
	}
	return nil, false
}

var errNotImplemented = errors.New("quota: not implemented")

// errBadConfig 是配置校验失败的哨兵错误，同时也是 *QuotaError：
// 调用方既可用 errors.Is 判定类型，也可用 AsQuotaError 提取原因。
var errBadConfig = &QuotaError{Reason: ReasonBadConfig}
